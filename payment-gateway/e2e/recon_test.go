//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"testing"
	"time"
)

func compose(t *testing.T, args ...string) string {
	t.Helper()
	full := append([]string{"compose", "-f", env("COMPOSE_FILE", "../../docker-compose.yml")}, args...)
	output, err := exec.Command("docker", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %v\n%s", full, err, output)
	}
	return string(output)
}

// TestReconciliationFindsInjectedBreaks proves the simulator's statement travels
// through the same Kafka ingestion, persisted job and read API as an operator run.
func TestReconciliationFindsInjectedBreaks(t *testing.T) {
	source := openAccount(t, "Recon activity", 500_00)
	payout := payoutTo(t, source, makeIBAN("DE", "370400440532011111"), 50_00)
	waitFor(t, 30*time.Second, "payout settled", func() bool {
		return transferStatus(t, payout) == "COMPLETED"
	})
	waitFor(t, 30*time.Second, "ledger outbox drained", func() bool {
		return getJSON(t, ledgerURL+"/api/invariants")["outbox_pending"] == float64(0)
	})

	seed := fmt.Sprint(time.Now().Unix() % 100000)
	compose(t, "exec", "-T", "reconciliation", "python", "-m", "recon",
		"generate-statement", "--out", "/data/e2e.csv", "--seed", seed)
	var truth struct {
		Injected []struct {
			Type      string `json:"type"`
			Currency  string `json:"currency"`
			Reference string `json:"reference"`
		} `json:"injected"`
	}
	groundTruth := compose(t, "exec", "-T", "reconciliation",
		"cat", "/data/e2e.csv.truth.json")
	if err := json.Unmarshal([]byte(groundTruth), &truth); err != nil {
		t.Fatal(err)
	}
	report := compose(t, "exec", "-T", "reconciliation", "python", "-m", "recon",
		"run", "--statement", "/data/e2e.csv")
	match := regexp.MustCompile(`Reconciliation run (\d+)`).FindStringSubmatch(report)
	if match == nil {
		t.Fatalf("no run id in report:\n%s", report)
	}
	run := getJSON(t, env("RECON_URL", "http://localhost:8083")+"/api/recon/runs/"+match[1])

	var found, expected []string
	closing := 0
	for _, rawBreak := range run["breaks"].([]any) {
		item := rawBreak.(map[string]any)
		switch item["break_type"] {
		case "CLOSING_BALANCE_MISMATCH":
			closing++
		case "OPENING_BALANCE_MISMATCH":
			t.Fatalf("opening balances must agree after ingestion catches up: %v", item)
		default:
			found = append(found, fmt.Sprint(item["break_type"], " ", item["reference"]))
		}
	}
	for _, injected := range truth.Injected {
		expected = append(expected, injected.Type+" "+injected.Reference)
	}
	sort.Strings(found)
	sort.Strings(expected)
	if fmt.Sprint(found) != fmt.Sprint(expected) {
		t.Fatalf("breaks found:\n%v\ninjected:\n%v\nreport:\n%s", found, expected, report)
	}
	if closing != 1 {
		t.Fatalf("want one closing-balance break for the injected currency, got %d\n%s",
			closing, report)
	}
	t.Logf("\n%s", report)
}
