package taskq

import (
	"fmt"
	"github.com/pkg/errors"
	"time"
)

type stats struct {
	taskProcessed int64
	averageTime   time.Duration
}

type worker[T any] struct {
	consume func(task T) error
	quit    chan chan stats
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
		stats := stats{
			taskProcessed: 0,
			averageTime:   0,
		}
		for {
			select {
			case statsc := <-w.quit:
				statsc <- stats
			case task := <-queue:
				func() {
					u := updateAverage(stats.taskProcessed, stats.averageTime)
					defer func() {
						u()
						checkError(survive())
					}()
					stats.taskProcessed++
					checkError(w.consume(task))
				}()
			}
		}
	}()
}

func (w worker[T]) stop() stats {
	statsc := make(chan stats, 1)
	w.quit <- statsc
	return <-statsc
}

func survive() error {
	if err := recover(); err != nil {
		return errors.Wrap(errors.New(fmt.Sprintf("%v", err)), "Crash detected when consuming a task")
	}
	return nil
}
