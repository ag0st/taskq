package taskq

import (
	"fmt"
	"github.com/pkg/errors"
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestNewQueue(t *testing.T) {
	queue := New[int](
		5, 1000, func(data int) error {
			time.Sleep(time.Second * time.Duration(rand.Intn(2)))
			log.Printf("%d \n", data)
			return nil
		},
	)
	var totalTest int64 = 100
	for i := int64(0); i < totalTest; i++ {
		queue.Push(int(i))
	}
	queue.Start()
	time.Sleep(time.Second * 20) // wait until it finishes
	statistics := queue.Stop()
	var totalTask int64 = 0
	for _, st := range statistics {
		totalTask += st.TaskProcessed
		if st.AverageTime > time.Second*2 {
			t.Fatalf("Cannot have an average time bigger than 2 seconds, got: [%v]", st.AverageTime)
		}
		log.Printf("%v", st.AverageTime) // average time of 1 second
	}
	if totalTask != totalTest {
		t.Fatalf("Number of task executed: [%d] expected [%d]", totalTask, totalTest)
	}
}

func TestError(t *testing.T) {
	queue := New[int](
		5, 1000, func(data int) error {
			// just produce an error
			return errors.New(fmt.Sprintf("Error processing %d", data))
		},
	)
	totalTest := 100
	for i := 0; i < totalTest; i++ {
		queue.Push(i)
	}
	queue.Start()

	for i := 0; i < totalTest; i++ {
		// must receive an error
		log.Println(<-queue.Errc)
	}
	queue.Stop()
}

func TestCrash(t *testing.T) {
	queue := New[int](
		5, 1000, func(data int) error {
			// just produce an error
			panic(fmt.Sprintf("Crash processing %d", data))
			return nil
		},
	)
	totalTest := 100
	for i := 0; i < totalTest; i++ {
		queue.Push(i)
	}
	queue.Start()

	for i := 0; i < totalTest; i++ {
		// must receive an error
		log.Println(<-queue.Panicc)
	}
	queue.Stop()
}
