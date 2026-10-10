package io.meridian.ledger;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.meridian.ledger.Model.CurrencyLimit;
import io.meridian.ledger.Model.PostCommand;
import io.meridian.ledger.Model.PostingLine;
import io.meridian.ledger.grpc.v1.Account;
import io.meridian.ledger.grpc.v1.AccountType;
import io.meridian.ledger.grpc.v1.Balance;
import io.meridian.ledger.grpc.v1.CreateAccountRequest;
import io.meridian.ledger.grpc.v1.GetAccountRequest;
import io.meridian.ledger.grpc.v1.GetTransactionRequest;
import io.meridian.ledger.grpc.v1.LedgerServiceGrpc;
import io.meridian.ledger.grpc.v1.PostTransactionRequest;
import io.meridian.ledger.grpc.v1.Posting;
import io.meridian.ledger.grpc.v1.ReverseTransactionRequest;
import io.meridian.ledger.grpc.v1.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.CannotCreateTransactionException;

import java.util.UUID;
import java.util.function.Supplier;

/** gRPC adapter: proto <-> domain mapping and error translation. No business logic lives here. */
@Component
public class GrpcLedgerEndpoint extends LedgerServiceGrpc.LedgerServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(GrpcLedgerEndpoint.class);
    private final LedgerService ledger;

    public GrpcLedgerEndpoint(LedgerService ledger) {
        this.ledger = ledger;
    }

    @Override
    public void createAccount(CreateAccountRequest req, StreamObserver<Account> out) {
        respond(out, () -> {
            var limits = req.getCurrenciesList().stream()
                    .map(c -> new CurrencyLimit(c.getCurrency(), c.hasOverdraftLimit() ? c.getOverdraftLimit() : null))
                    .toList();
            Model.AccountType type = req.getType() == AccountType.ACCOUNT_TYPE_UNSPECIFIED || req.getType() == AccountType.UNRECOGNIZED
                    ? null : Model.AccountType.valueOf(req.getType().name());
            return toProto(ledger.createAccount(req.getId(), req.getName(), type, limits));
        });
    }

    @Override
    public void getAccount(GetAccountRequest req, StreamObserver<Account> out) {
        respond(out, () -> toProto(ledger.getAccount(req.getId())));
    }

    @Override
    public void postTransaction(PostTransactionRequest req, StreamObserver<Transaction> out) {
        respond(out, () -> {
            var postings = req.getPostingsList().stream()
                    .map(p -> new PostingLine(p.getAccountId(), p.getCurrency(), p.getAmount()))
                    .toList();
            var cmd = new PostCommand(req.getIdempotencyKey(), req.getType(), req.getDescription(), postings, req.getMetadataMap());
            return toProto(ledger.post(cmd).transaction());
        });
    }

    @Override
    public void reverseTransaction(ReverseTransactionRequest req, StreamObserver<Transaction> out) {
        respond(out, () -> toProto(ledger.reverse(req.getIdempotencyKey(), parseId(req.getTransactionId()), req.getReason()).transaction()));
    }

    @Override
    public void getTransaction(GetTransactionRequest req, StreamObserver<Transaction> out) {
        respond(out, () -> toProto(ledger.getTransaction(parseId(req.getId()))));
    }

    private static UUID parseId(String id) {
        try {
            return UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            throw new LedgerException(LedgerException.Reason.INVALID_ARGUMENT, "invalid transaction id: " + id);
        }
    }

    private static <T> void respond(StreamObserver<T> out, Supplier<T> call) {
        T result;
        try {
            result = call.get();
        } catch (LedgerException e) {
            out.onError(businessError(e));
            return;
        } catch (TransientDataAccessException | CannotGetJdbcConnectionException | CannotCreateTransactionException e) {
            // Lock/statement timeouts, lost connections: the DB transaction rolled back, the caller may retry.
            log.warn("transient database failure: {}", e.getMessage());
            out.onError(Status.UNAVAILABLE.withDescription("ledger database unavailable, retry").asRuntimeException());
            return;
        } catch (RuntimeException e) {
            log.error("unexpected ledger failure", e);
            out.onError(Status.INTERNAL.withDescription("internal ledger error").asRuntimeException());
            return;
        }
        out.onNext(result);
        out.onCompleted();
    }

    private static RuntimeException businessError(LedgerException e) {
        Code code = switch (e.reason) {
            case INVALID_ARGUMENT, UNBALANCED -> Code.INVALID_ARGUMENT;
            case ACCOUNT_NOT_FOUND, TRANSACTION_NOT_FOUND -> Code.NOT_FOUND;
            case IDEMPOTENCY_KEY_REUSED -> Code.ALREADY_EXISTS;
            case INSUFFICIENT_FUNDS, CURRENCY_NOT_ENABLED, ALREADY_REVERSED -> Code.FAILED_PRECONDITION;
        };
        var info = ErrorInfo.newBuilder().setReason(e.reason.name()).setDomain("ledger.meridian.io").build();
        var status = com.google.rpc.Status.newBuilder()
                .setCode(code.getNumber())
                .setMessage(e.getMessage())
                .addDetails(Any.pack(info))
                .build();
        return StatusProto.toStatusRuntimeException(status);
    }

    static Account toProto(Model.Account a) {
        var b = Account.newBuilder().setId(a.id()).setName(a.name()).setType(AccountType.valueOf(a.type().name()));
        for (Model.Balance bal : a.balances()) {
            var pb = Balance.newBuilder().setCurrency(bal.currency()).setBalance(bal.balance()).setVersion(bal.version());
            if (bal.overdraftLimit() != null) pb.setOverdraftLimit(bal.overdraftLimit());
            b.addBalances(pb);
        }
        return b.build();
    }

    static Transaction toProto(Model.Transaction t) {
        var b = Transaction.newBuilder()
                .setId(t.id().toString())
                .setIdempotencyKey(t.idempotencyKey())
                .setType(t.type())
                .setDescription(t.description())
                .putAllMetadata(t.metadata())
                .setBookedAt(t.bookedAt().toString());
        if (t.reversesId() != null) b.setReversesId(t.reversesId().toString());
        for (PostingLine p : t.postings()) {
            b.addPostings(Posting.newBuilder().setAccountId(p.accountId()).setCurrency(p.currency()).setAmount(p.amount()));
        }
        return b.build();
    }
}
