package trace

import (
	"sync"
)

// TODO: Move to its own package as needed, since this is a helper for the trace that could be reused.

type JobStatus int

const (
	JobStatusUnspecified JobStatus = iota
	JobStatusQueued
	JobStatusRunning
	JobStatusCompleted
	JobStatusFailed
)

// WorkerJob is an interface for jobs that can be run in the WorkerPool.
// The workers in the WorkerPool run the job's Run method and signal on the doneChan when all are done.
// Run takes *WorkerPool so new jobs can be spawned from within the execution.
// Errors returned by the Run method are sent to the errorChan so they can be handled.
type WorkerJob interface {
	Run(*WorkerPool) error
}

// TODO: WorkerPool is a simple job runner.
// WorkerPool starts a worker pool where WorkerJobs can be executed. WorkerPool.NewJob adds new jobs to the queue.
// Contains doneChan and errorChan channels where completion and errors are signalled.
// Once the initial jobs have been called, use WorkerPool.WaitAndClose() to wait on jobs to finish and get a done signal.
// TODO: Make interface
type WorkerPool struct {
	// workerCount is the total number of workers in the WorkerPool.
	workerCount int
	// jobs contains the WorkerJob queue.
	jobs chan WorkerJob
	// wg is a WaitGroup to sync the queue.
	wg *sync.WaitGroup
	// jobsStatus contains the status of each job. TODO: Consider if this is necessary
	jobsStatus map[WorkerJob]JobStatus

	// doneChan will send a true value when all the jobs are done.
	doneChan chan bool
	// errorChan will send errors from the jobs.
	errorChan chan error
}

// NewWorkerPool creates a new WorkerPool with workerCount workers. These will execute jobs in the jobs channel.
// The jobs channel will block once there's no more workers available to pick the jobs up.
func NewWorkerPool(workerCount int) *WorkerPool {
	pool := &WorkerPool{
		workerCount: workerCount,
		wg:          &sync.WaitGroup{},
		jobs:        make(chan WorkerJob, workerCount),
		jobsStatus:  make(map[WorkerJob]JobStatus),
		doneChan:    make(chan bool, 1),
		errorChan:   make(chan error),
	}

	return pool
}

// Start starts the pool's workers
func (pool *WorkerPool) Start() {
	// Start workers
	for i := 0; i < pool.workerCount; i++ {
		go pool.newWorker()
	}
	pool.WaitAndClose()

	return
}

// SendError sends an error to the error channel so it can be handled.
// This method doesn't block since errors might not be handled.
func (pool *WorkerPool) SendError(err error) {
	go func() {
		pool.errorChan <- err
	}()
}

// newWorker starts a new worker capable of running WorkerJobs
func (pool *WorkerPool) newWorker() {
	for job := range pool.jobs {
		pool.SetJobStatus(job, JobStatusRunning)
		err := job.Run(pool)

		// Send error to the error channel so it can be processed
		if err != nil {
			pool.SetJobStatus(job, JobStatusFailed)
			pool.SendError(err)
		} else {
			pool.SetJobStatus(job, JobStatusCompleted)
		}
		pool.wg.Done()
	}
}

// NewJob adds a new job to the WorkerPool queue so it can be executed by the workers.
func (pool *WorkerPool) NewJob(job WorkerJob) {
	pool.wg.Add(1)
	go func() {
		pool.SetJobStatus(job, JobStatusQueued)
		pool.jobs <- job
	}()
}

// WaitAndClose waits for all jobs to finish, closes the job channel and signals done. Should be called once the initial jobs have been registered.
func (pool *WorkerPool) WaitAndClose() {
	go func() {
		pool.wg.Wait()
		close(pool.jobs)
		pool.doneChan <- true
	}()
}

func (pool *WorkerPool) SetJobStatus(job WorkerJob, status JobStatus) {
	pool.jobsStatus[job] = status
}

func (pool *WorkerPool) GetJobStatus(job WorkerJob) JobStatus {
	if status, ok := pool.jobsStatus[job]; ok {
		return status
	}
	return JobStatusUnspecified
}
