package taskq

type TaskQ[T any] struct {
	queue    chan T
	workers  []worker[T]
	Errc     chan error
	workerFn func(job T) error
	scaling  Scaling
}

func New[T any](scalingOptions Scaling, capacity int, workerFnc func(job T) error) TaskQ[T] {
	t := TaskQ[T]{
		queue:     make(chan T, capacity),
		workers:   make([]worker[T], 0, scalingOptions.MaxReplica),
		Errc:      make(chan error),
		workerFnc: workerFnc,
		scaling:   scalingOptions,
	}
	go t.monitor()
	return t
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
