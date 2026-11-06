package harness

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"yodoq/solution"
)

// TestRunScenario_MatchesLevel2Literals builds a ScenarioSpec with level 2's
// exact literals from main.go (jobCount=200, workerCount=5, leaseMS=1000,
// processingMs=100, crashRate=0.20, ackDropRate=0.0, seed=42) and asserts
// zero jobs are lost, exactly as main.go's level2() already informally
// checks.
func TestRunScenario_MatchesLevel2Literals(t *testing.T) {
	clock := &TestClock{}
	q := solution.New(clock)
	spec := ScenarioSpec{
		Name:             "level2",
		Seed:             42,
		JobCount:         200,
		WorkerCount:      5,
		LeaseMS:          1000,
		ProcessingTimeMS: 100,
		CrashRate:        0.20,
		AckDropRate:      0.0,
	}

	ledger, jobIDs := RunScenario(q, clock, spec)

	if len(jobIDs) != spec.JobCount {
		t.Fatalf("got %d job IDs, want %d", len(jobIDs), spec.JobCount)
	}
	if lost := ledger.Lost(jobIDs); lost != 0 {
		t.Fatalf("lost %d jobs, want 0", lost)
	}
	if excess := ledger.Excess(jobIDs); float64(excess) > float64(spec.JobCount)*spec.CrashRate*2 {
		t.Fatalf("excess executions %d exceeds bound %.0f", excess, float64(spec.JobCount)*spec.CrashRate*2)
	}
}

// TestGoRunOutputIsByteIdentical runs `go run .` from the repo root and
// asserts it still succeeds. This is a lightweight companion to the
// before/after byte-diff that was performed manually during the
// internal/harness extraction (captured in implementation.md); it is not a
// full golden-file comparison (the harness module is probabilistic-but
// -seeded, and running `go run .` from `go test` doubles build time), but it
// does catch "go run . no longer runs at all" regressions in CI.
func TestGoRunOutputIsByteIdentical(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping go run . smoke test in -short mode")
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine caller file")
	}
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")

	cmd := exec.Command("go", "run", ".")
	cmd.Dir = repoRoot
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		t.Fatalf("go run . failed: %v\noutput:\n%s", err, out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("ASSESSMENT COMPLETE")) {
		t.Fatalf("go run . output missing expected banner; got:\n%s", out.String())
	}
}
