package main

import (
	"fmt"
	"strings"

	"yodoq/internal/harness"
	"yodoq/solution"
)

// =============================================================================
// Logger
// =============================================================================

func info(msg string)     { fmt.Printf("[INFO] %s\n", msg) }
func successf(msg string) { fmt.Printf("[SUCCESS] ✓ %s\n", msg) }
func errf(msg string)     { fmt.Printf("[ERROR] ✗ %s\n", msg) }
func warn(msg string)     { fmt.Printf("[WARNING] ⚠ %s\n", msg) }

func section(title string) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("📬 YODOQ %s\n", strings.ToUpper(title))
	fmt.Println(strings.Repeat("=", 60))
}

func intro() {
	fmt.Println(`
██╗   ██╗ ██████╗ ██████╗  ██████╗  ██████╗
╚██╗ ██╔╝██╔═══██╗██╔══██╗██╔═══██╗██╔═══██╗
 ╚████╔╝ ██║   ██║██║  ██║██║   ██║██║   ██║
  ╚██╔╝  ██║   ██║██║  ██║██║   ██║██║▄▄ ██║
   ██║   ╚██████╔╝██████╔╝╚██████╔╝╚██████╔╝
   ╚═╝    ╚═════╝ ╚═════╝  ╚═════╝  ╚══▀▀═╝

   Your Own Don't-Drop Queue`)
	fmt.Println("📬 Welcome to YODOQ — where workers crash and acks vanish.")
	fmt.Println("💪 Let's see how reliable your queue really is...")
}

// =============================================================================
// Levels
// =============================================================================

// Level 1: basic correctness, no failures injected.
func level1() {
	section("Level 1: Basic YODOQ Operations")

	clock := &harness.TestClock{}
	q := solution.New(clock)

	info("Testing enqueue + lease + ack...")
	id, err := q.Enqueue([]byte("hello"))
	if err != nil {
		errf(fmt.Sprintf("Enqueue failed: %v", err))
		return
	}
	successf(fmt.Sprintf("Enqueued job %s", id))

	job, err := q.Lease("worker-1", 5000)
	if err != nil || job == nil {
		errf(fmt.Sprintf("Lease failed: %v", err))
		return
	}
	if string(job.Payload) != "hello" {
		errf("Leased job payload mismatch")
		return
	}
	successf("Leased job with correct payload")

	if err := q.Ack("worker-1", job.ID); err != nil {
		errf(fmt.Sprintf("Ack failed: %v", err))
		return
	}
	successf("Acked job successfully")

	if j, _ := q.Lease("worker-1", 5000); j == nil {
		successf("Acked job is not re-leasable")
	} else {
		errf("Acked job should not be re-leasable")
	}

	info("Testing multiple jobs...")
	for i := 0; i < 10; i++ {
		_, _ = q.Enqueue([]byte(fmt.Sprintf("job-%d", i)))
	}
	leased := 0
	for {
		j, _ := q.Lease("worker-1", 5000)
		if j == nil {
			break
		}
		_ = q.Ack("worker-1", j.ID)
		leased++
	}
	if leased == 10 {
		successf(fmt.Sprintf("Leased and acked all %d jobs", leased))
	} else {
		errf(fmt.Sprintf("Only leased %d/10 jobs", leased))
	}

	info("Testing fail re-lease...")
	_, _ = q.Enqueue([]byte("to-fail"))
	j1, _ := q.Lease("worker-1", 5000)
	if j1 == nil {
		errf("Could not lease the job we just enqueued")
		return
	}
	_ = q.Fail("worker-1", j1.ID)
	j2, _ := q.Lease("worker-2", 5000)
	if j2 != nil && j2.ID == j1.ID {
		successf("Failed job was re-leased to a different worker")
	} else {
		errf("Failed job was not re-leased")
	}
	if j2 != nil {
		_ = q.Ack("worker-2", j2.ID)
	}

	info("Testing stale ack rejection (lease expiry)...")
	_, _ = q.Enqueue([]byte("stale"))
	s1, _ := q.Lease("worker-1", 1000)
	if s1 == nil {
		errf("Could not lease job for stale-ack test")
		return
	}
	clock.Advance(2000)
	s2, _ := q.Lease("worker-2", 5000)
	if s2 != nil && s2.ID == s1.ID {
		if err := q.Ack("worker-1", s1.ID); err != nil {
			successf("Stale ack from expired lease holder was rejected")
		} else {
			errf("Stale ack from expired lease holder was accepted (BAD)")
		}
		_ = q.Ack("worker-2", s2.ID)
	} else {
		warn("Lease did not transfer after expiry — Level 2 will surface this")
	}
}

// Level 2: 20% crash rate, no ack drops. Goal: zero lost jobs.
func level2() {
	section("Level 2: YODOQ Worker Crash Recovery")

	clock := &harness.TestClock{}
	q := solution.New(clock)

	const jobCount = 200
	const crashRate = 0.20
	spec := harness.ScenarioSpec{
		Name:             "level2",
		Seed:             42,
		JobCount:         jobCount,
		WorkerCount:      5,
		LeaseMS:          1000,
		ProcessingTimeMS: 100,
		CrashRate:        crashRate,
		AckDropRate:      0.0,
	}

	info(fmt.Sprintf("Enqueuing %d jobs...", jobCount))
	info(fmt.Sprintf("Running 5 workers with %.0f%% crash rate...", crashRate*100))
	ledger, jobIDs := harness.RunScenario(q, clock, spec)

	lost := ledger.Lost(jobIDs)
	excess := ledger.Excess(jobIDs)
	total := ledger.Total()

	info("Results:")
	info(fmt.Sprintf("  - Total executions:    %d", total))
	info(fmt.Sprintf("  - Jobs lost (0 runs):  %d / %d", lost, jobCount))
	info(fmt.Sprintf("  - Excess executions:   %d", excess))
	dupRate := 0.0
	if jobCount > 0 {
		dupRate = (float64(total)/float64(jobCount) - 1) * 100
	}
	info(fmt.Sprintf("  - Effective dup rate:  %.1f%%", dupRate))

	if lost == 0 {
		successf("No jobs were lost — every job ran at least once!")
	} else {
		errf(fmt.Sprintf("%d jobs were lost forever (BAD).", lost))
	}
	if float64(excess) <= float64(jobCount)*crashRate*2 {
		successf("Duplication is bounded reasonably given crash rate.")
	} else {
		warn(fmt.Sprintf("High duplication: %d excess executions.", excess))
	}
}

// Level 3: sweep crash + ack-drop rates from 0.1% to 50%.
func level3() {
	section("Level 3: YODOQ Reliability Stress Sweep")

	rates := []float64{0.001, 0.01, 0.05, 0.10, 0.20, 0.30, 0.50}
	const jobCount = 200

	rateStr := strings.Builder{}
	for i, r := range rates {
		if i > 0 {
			rateStr.WriteString(", ")
		}
		rateStr.WriteString(fmt.Sprintf("%.1f%%", r*100))
	}
	info(fmt.Sprintf("Sweeping crash + ack-drop rates: %s", rateStr.String()))

	for _, rate := range rates {
		fmt.Println()
		info(fmt.Sprintf("Testing rate: crash=%.2f%%, ack-drop=%.2f%%", rate*100, rate*100))

		clock := &harness.TestClock{}
		q := solution.New(clock)
		spec := harness.ScenarioSpec{
			Name:             "level3",
			Seed:             int64(rate * 10000),
			JobCount:         jobCount,
			WorkerCount:      5,
			LeaseMS:          1000,
			ProcessingTimeMS: 100,
			CrashRate:        rate,
			AckDropRate:      rate,
		}

		ledger, jobIDs := harness.RunScenario(q, clock, spec)
		if len(jobIDs) == 0 {
			warn("Enqueue is not implemented — skipping this rate.")
			continue
		}

		lost := ledger.Lost(jobIDs)
		excess := ledger.Excess(jobIDs)
		total := ledger.Total()
		dup := 0.0
		if jobCount > 0 {
			dup = (float64(total)/float64(jobCount) - 1) * 100
		}

		info("  Results:")
		info(fmt.Sprintf("    - Jobs lost:        %d / %d (%.1f%%)", lost, len(jobIDs), float64(lost)/float64(len(jobIDs))*100))
		info(fmt.Sprintf("    - Excess execs:     %d", excess))
		info(fmt.Sprintf("    - Duplication:      %.1f%%", dup))
		info(fmt.Sprintf("    - Total executions: %d", total))

		if lost > 0 {
			errf(fmt.Sprintf("Jobs lost at %.1f%% — reliability compromised.", rate*100))
		}
	}
}

func main() {
	intro()
	section("Reliability Assessment Protocol")
	level1()
	level2()
	level3()
	section("YODOQ Assessment Complete")
}
