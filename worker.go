package taskq

import (
	"fmt"
	"github.com/pkg/errors"
	"time"
)

type Stats struct {
	TaskProcessed int64
	AverageTime   time.Duration
}

type worker[T any] struct {
	consume func(task T) error
	quit    chan chan Stats
}

func newWorker[T any](workerFnc func(task T) error) worker[T] {
	return worker[T]{
		consume: workerFnc,
		quit:    make(chan chan Stats, 1),
	}
}

func updateAverage(times int64, average time.Duration) func() time.Duration {
	t1 := time.Now()
	return func() time.Duration {
		dur := time.Since(t1)
		return time.Duration((average.Milliseconds()*times+dur.Milliseconds())/(times+1)) * time.Millisecond
	}
}

func (w worker[T]) start(queue chan T, errc chan error) {
	checkError := func(err error) {
		if err != nil {
			errc <- err
		}
	}
	go func() {
		stats := Stats{
			TaskProcessed: 0,
			AverageTime:   0,
		}
		for {
			select {
			case statsc := <-w.quit:
				statsc <- stats
			case task := <-queue:
				func() {
					u := updateAverage(stats.TaskProcessed, stats.AverageTime)
					defer func() {
						// update the average time
						stats.AverageTime = u()
						// recover is something went wrong, don't want to crash the worker itself.
						if err := recover(); err != nil {
							checkError(
								errors.Wrap(
									errors.New(fmt.Sprintf("%v", err)), "Crash detected when consuming a task",
								),
							)
						}
					}()
					stats.TaskProcessed++
					checkError(w.consume(task))
				}()
			}
		}
	}()
}

func (w worker[T]) stop() Stats {
	statsc := make(chan Stats, 1)
	w.quit <- statsc
	return <-statsc
}
