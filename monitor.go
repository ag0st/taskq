package taskq

import (
	"time"
)

type Scaling struct {
	MaxReplica int
	MinReplica int
	// CheckIntervalStr is the string form of duration, please use CheckInterval for the parsed form
	// is of type 2s, 2h, 5min, ....
	CheckInterval time.Duration
	ScaleDown     Policy
	ScaleUp       Policy
}

type Policy struct {
	// PeriodStr is the string form of duration, please use Period for the parsed form
	// is of type 2s, 2h, 5min, ....
	Period time.Duration
	Value  int
	Many   int
}

// monitor is the implementation of the HCA (Horizontal Caller Autoscaler)
func (t *TaskQ[T]) monitor() {
	createAndStartWorker := func() *worker[T] {
		w := worker[T]{
			consume: t.workerFn,
		}
		w.start()
		return &w
	}
	// creating the minimum replica of workers
	for i := 0; i < t.scaling.MinReplica; i++ {
		t.workers = append(t.workers, *createAndStartWorker())
	}

	// initialize both time for scale period
	lastTimeScaleUp := time.Now()
	lastTimeScaleDown := time.Now()

	// Main loop for monitoring
	for {
		// wait before checking again
		time.Sleep(t.scaling.CheckInterval)

		// -----------------------------
		//         SCALE UP PART
		// -----------------------------
		// If period between last scale up is > scaleUp.Period && condition respected
		if time.Since(lastTimeScaleUp) > t.scaling.ScaleUp.Period && len(t.queue) > t.scaling.ScaleUp.Value {
			n := min(t.scaling.MaxReplica-len(t.workers), t.scaling.ScaleUp.Many)
			for i := 0; i < n; i++ {
				t.workers = append(t.workers, *createAndStartWorker())
			}
			// update last time scaled up if really scaled up
			if n > 0 {
				lastTimeScaleUp = time.Now()
			}
			continue
		}
		// -----------------------------
		//         SCALE DOWN PART
		// -----------------------------
		if time.Now().Sub(lastTimeScaleDown) > t.scaling.ScaleDown.Period && len(t.queue) <= t.scaling.ScaleDown.Value {
			n := min(len(t.workers)-t.scaling.MinReplica, t.scaling.ScaleDown.Many)
			for i := 0; i < n; i++ { // stop and delete all workers
				// stop first worker when it will finish its task
				_ = t.workers[0].stop()
				// remove the first worker from the worker list
				t.workers = t.workers[1:]
			}
			// update last time scaled down if really scaled down
			if n > 0 {
				lastTimeScaleDown = time.Now()
			}
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
