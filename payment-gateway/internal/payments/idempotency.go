package payments

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

// LockTTL is how long a request owns its idempotency key before a retry may take over and
// re-drive the operation (safe because every ledger call uses a deterministic key).
const LockTTL = 10 * time.Second

// Response is what the API returns for a mutating request, and what is replayed on retries.
type Response struct {
	Code     int
	Body     []byte
	Replayed bool
}

// Problem is an RFC 7807 error. Returned from a BeginFunc it is stored and replayed like a success.
type Problem struct {
	Status int
	Code   string
	Detail string
}

func (p *Problem) Error() string { return p.Code + ": " + p.Detail }

func problem(status int, code, format string, args ...any) *Problem {
	return &Problem{Status: status, Code: code, Detail: fmt.Sprintf(format, args...)}
}

func (p *Problem) Response() Response {
	body, _ := json.Marshal(map[string]any{
		"type":   "https://meridian.example/problems/" + p.Code,
		"title":  http.StatusText(p.Status),
		"status": p.Status,
		"detail": p.Detail,
		"code":   p.Code,
	})
	return Response{Code: p.Status, Body: body}
}

// BeginFunc runs inside the transaction that claimed the idempotency key. It either finishes
// the request (returns a Response) or registers an in-flight resource (kind, id) to be driven.
type BeginFunc func(ctx context.Context, tx pgx.Tx) (resp *Response, kind, id string, err error)

// Execute gives a mutating request exactly-once semantics per (client, key):
//
//  1. claim the key and run begin, in one DB transaction;
//  2. drive the resource through the ledger (outside any DB transaction);
//  3. the driver stores the final response on the key in the same transaction as the state change.
//
// A retry replays the stored response; a retry of a request whose owner died re-drives it.
func (s *Service) Execute(ctx context.Context, clientID, key, method, path string, body []byte, begin BeginFunc) (Response, error) {
	if len(key) == 0 || len(key) > 255 {
		return problem(400, "idempotency_key_required", "Idempotency-Key header must be 1..255 characters").Response(), nil
	}
	sum := sha256.Sum256(append([]byte(method+" "+path+"\n"), body...))
	hash := hex.EncodeToString(sum[:])

	var (
		out        *Response
		kind, id   string
		transient  bool
	)
	err := pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		out, kind, id, transient = nil, "", "", false
		tag, err := tx.Exec(ctx, `
			INSERT INTO idempotency_keys (client_id, key, request_method, request_path, request_hash, status)
			VALUES ($1, $2, $3, $4, $5, 'in_progress') ON CONFLICT DO NOTHING`, clientID, key, method, path, hash)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return s.resumeExisting(ctx, tx, clientID, key, hash, &out, &kind, &id)
		}
		resp, k, rid, err := begin(ctx, tx)
		var p *Problem
		if errors.As(err, &p) {
			r := p.Response()
			resp, err = &r, nil
		}
		if err != nil {
			return err
		}
		if resp != nil {
			out = resp
			if resp.Code == http.StatusConflict {
				// A conflict describes a transient state (e.g. another operation in flight): don't pin it to the key.
				transient = true
				return errRollback
			}
			_, err := tx.Exec(ctx, `
				UPDATE idempotency_keys SET status = 'completed', resource_type = $3, resource_id = $4,
				       response_code = $5, response_body = $6
				WHERE client_id = $1 AND key = $2`, clientID, key, k, rid, resp.Code, resp.Body)
			return err
		}
		kind, id = k, rid
		_, err = tx.Exec(ctx, `UPDATE idempotency_keys SET resource_type = $3, resource_id = $4 WHERE client_id = $1 AND key = $2`,
			clientID, key, kind, id)
		return err
	})
	if errors.Is(err, errRollback) && transient {
		return *out, nil
	}
	if err != nil {
		return Response{}, err
	}
	if out != nil {
		return *out, nil
	}

	if err := s.Drive(ctx, kind, id); err != nil {
		s.Log.Warn("drive failed, recovery will retry", "kind", kind, "id", id, "err", err)
	}
	var code int
	var stored []byte
	var status string
	err = s.DB.QueryRow(ctx, `SELECT status, coalesce(response_code, 0), response_body FROM idempotency_keys WHERE client_id = $1 AND key = $2`,
		clientID, key).Scan(&status, &code, &stored)
	if err != nil {
		return Response{}, err
	}
	if status != "completed" {
		r := problem(503, "processing", "%s %s is still processing (a dependency is unavailable); retry with the same Idempotency-Key", kind, id).Response()
		return r, nil
	}
	return Response{Code: code, Body: stored}, nil
}

var errRollback = errors.New("rollback")

func (s *Service) resumeExisting(ctx context.Context, tx pgx.Tx, clientID, key, hash string, out **Response, kind, id *string) error {
	var (
		storedHash, status string
		code               *int
		body               []byte
		rkind, rid         *string
		lockedAt           time.Time
	)
	err := tx.QueryRow(ctx, `
		SELECT request_hash, status, response_code, response_body, resource_type, resource_id, locked_at
		FROM idempotency_keys WHERE client_id = $1 AND key = $2 FOR UPDATE`, clientID, key).
		Scan(&storedHash, &status, &code, &body, &rkind, &rid, &lockedAt)
	if err != nil {
		return err
	}
	if storedHash != hash {
		r := problem(422, "idempotency_key_reused", "Idempotency-Key %q was already used with a different request", key).Response()
		*out = &r
		return nil
	}
	if status == "completed" {
		idempotentReplays.Inc()
		*out = &Response{Code: *code, Body: body, Replayed: true}
		return nil
	}
	if time.Since(lockedAt) < LockTTL || rkind == nil {
		r := problem(409, "request_in_progress", "a request with this Idempotency-Key is still in progress").Response()
		*out = &r
		return nil
	}
	// The original request's owner is gone: take over and re-drive.
	if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET locked_at = now() WHERE client_id = $1 AND key = $2`, clientID, key); err != nil {
		return err
	}
	*kind, *id = *rkind, *rid
	return nil
}

// completeIdempotency stores the final response on the key(s) waiting on this resource.
// It runs in the same transaction as the resource's final state change.
func completeIdempotency(ctx context.Context, tx pgx.Tx, kind, id string, code int, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE idempotency_keys SET status = 'completed', response_code = $3, response_body = $4
		WHERE resource_type = $1 AND resource_id = $2 AND status = 'in_progress'`, kind, id, code, b)
	return err
}

// completeIdempotencyProblem is completeIdempotency for an error outcome.
func completeIdempotencyProblem(ctx context.Context, tx pgx.Tx, kind, id string, p *Problem) error {
	r := p.Response()
	_, err := tx.Exec(ctx, `
		UPDATE idempotency_keys SET status = 'completed', response_code = $3, response_body = $4
		WHERE resource_type = $1 AND resource_id = $2 AND status = 'in_progress'`, kind, id, r.Code, r.Body)
	return err
}
