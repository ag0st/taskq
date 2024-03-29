package taskq

type TaskQ[T any] struct {
	queue         chan T
	workers       []worker[T]
	Errc          chan error
	Panicc        chan error
	OnWorkerStart func()
	OnWorkerStop  func()
}

func New[T any](workers, capacity int, workerFnc func(job T) error) TaskQ[T] {
	t := TaskQ[T]{
		queue:   make(chan T, capacity),
		workers: make([]worker[T], workers),
		Errc:    make(chan error),
		Panicc:  make(chan error),
	}
	for i := 0; i < workers; i++ {
		t.workers[i] = newWorker(workerFnc)
	}
	return t
}

// Start starts all the workers inside the queue.
func (t TaskQ[T]) Start() {
	for _, w := range t.workers {
		w.start(t.queue, t.Errc, t.Panicc)
		// execute on start
		if t.OnWorkerStart != nil {
			t.OnWorkerStart()
		}
	}
}

// Stop stops all the workers of the queue.
func (t TaskQ[T]) Stop() []Stats {
	var res []Stats
	for _, w := range t.workers {
		// for now ignore the stats
		res = append(res, w.stop())
		// execute on stop
		if t.OnWorkerStop != nil {
			t.OnWorkerStop()
		}
	}
	return res
}

// PushNonBlocking tries to push into the task queue, but if the queue is full, don't block.
// return true if the task has been correctly pushed, false if no more space in the queue.
func (t TaskQ[T]) PushNonBlocking(task T) bool {
	select {
	case t.queue <- task:
		return true
	default:
		return false
	}
}

// Push is a blocking method trying to push into the task queue.
func (t TaskQ[T]) Push(task T) {
	t.queue <- task
}
