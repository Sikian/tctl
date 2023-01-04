package trace

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
)

// SimpleSyncCountJob is a job that counts each time the Run method is called.
type SimpleSyncCountJob struct {
	count int
}

func (job *SimpleSyncCountJob) Run(_ *WorkerPool) error {
	job.count = job.count + 1
	return nil
}

func TestWorkerPool_SimpleSyncCount(t *testing.T) {
	pool := NewWorkerPool(1)
	job := &SimpleSyncCountJob{count: 0}
	pool.NewJob(job)
	pool.NewJob(job)
	pool.Start()

	<-pool.doneChan
	require.Equal(t, 2, job.count)
}

// SimpleUpdateJob is a job that sends the given values to the updateChan one after the other.
type SimpleUpdateJob struct {
	values     []string
	updateChan chan string
}

func NewSimpleUpdateJob(values []string) (*SimpleUpdateJob, chan string) {
	updateChan := make(chan string)
	return &SimpleUpdateJob{values: values, updateChan: updateChan}, updateChan
}

func (job *SimpleUpdateJob) Run(_ *WorkerPool) error {
	for _, word := range job.values {
		job.updateChan <- word
	}
	return nil
}

func TestWorkerPool_SimpleUpdateJob(t *testing.T) {
	pool := NewWorkerPool(1)

	words := []string{"foo", "bar", "baz"}
	job, updateChan := NewSimpleUpdateJob(words)

	pool.NewJob(job)
	pool.Start()

	count := 0
	for {
		select {
		case word := <-updateChan:
			require.Equal(t, words[count], word)
			count++
		case <-pool.doneChan:
			require.Equal(t, len(words), count)
			require.Equal(t, JobStatusCompleted, pool.GetJobStatus(job))
			return
		}
	}
}

// PrimeFactorizationJob is a job that send the prime factors of a value. This job is meant to test spawning child jobs.
type PrimeFactorizationJob struct {
	value      int
	updateChan chan int
}

func NewPrimeFactorizationJob(value int) (*PrimeFactorizationJob, chan int) {
	updateChan := make(chan int, 1)
	return &PrimeFactorizationJob{value, updateChan}, updateChan
}

func (job *PrimeFactorizationJob) Run(pool *WorkerPool) error {
	easyPrimes := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
	for _, p := range easyPrimes {
		if p >= job.value {
			break
		}
		if job.value%p == 0 {
			// spawn a child job
			child, updateChan := NewPrimeFactorizationJob(job.value / p)
			pool.NewJob(child)
			go func() {
				for msg := range updateChan {
					job.updateChan <- msg
				}
			}()
			job.updateChan <- p
			return nil
		}
	}
	job.updateChan <- job.value
	return nil
}

func TestWorkerPool_PrimeFactorizationJob(t *testing.T) {
	tests := []struct {
		value  int
		primes []int
	}{
		{
			value:  10,
			primes: []int{2, 5},
		},
		{
			value:  122,
			primes: []int{},
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%d", tc.value), func(t *testing.T) {
			job, updateChan := NewPrimeFactorizationJob(tc.value)

			pool := NewWorkerPool(10)
			pool.NewJob(job)
			pool.Start()

			var res []int
			for {
				select {
				case prime := <-updateChan:
					res = append(res, prime)
				case <-pool.doneChan:
					sort.Ints(res)
					sort.Ints(tc.primes)
					require.EqualValues(t, tc.primes, res)
					break
				}
			}
		})
	}
}
