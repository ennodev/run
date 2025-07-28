package run

import (
	gocontext "context"
	"time"
)

// Job implements a job that runs once with retry capability
type Job struct {
	id           string
	enabled      bool
	runFunc      func(gocontext.Context) error
	gracefulStop chan struct{}
	retry        int
	retryDelay   time.Duration
	timeout      time.Duration
	logger       ILogger
}

// JobOption defines functional options for configuring Job
type JobOption func(*Job)

// WithJobRetryDelay sets the delay between retries
func WithJobRetryDelay(delay time.Duration) JobOption {
	return func(t *Job) {
		t.retryDelay = delay
	}
}

// WithJobTimeout sets execution timeout for the job
func WithJobTimeout(timeout time.Duration) JobOption {
	return func(job *Job) {
		job.timeout = timeout
	}
}

func NewJob(
	id string,
	enabled bool,
	runFunc func(gocontext.Context) error,
	retry int,
	logger ILogger,
	options ...JobOption,
) IJob {
	job := &Job{
		logger:       logger,
		id:           id,
		enabled:      enabled,
		runFunc:      runFunc,
		retry:        retry,
		retryDelay:   time.Minute,     // Default retry delay of 1 minute
		timeout:      5 * time.Minute, // Default timeout of 5 minutes
		gracefulStop: make(chan struct{}),
	}
	for _, option := range options {
		option(job)
	}
	return job
}

func (o *Job) Id() string {
	return o.id
}

func (o *Job) Enabled() bool {
	return o.enabled
}

func (o *Job) Run() {
	if !o.enabled {
		o.logger.Infof("%v job is disabled", o.id)
		return
	}
	o.logger.Infof("%v job started", o.id)
	defer o.logger.Infof("[Run] %v job stopped", o.id)
	startTime := time.Now()

	for i := 0; i < o.retry; i++ {
		execCtx, cancel := gocontext.WithTimeout(gocontext.Background(), o.timeout)
		err := o.runFunc(execCtx)
		cancel()
		if err != nil {
			o.logger.Errorf("%v job failed (attempt %d/%d): %v", o.id, i+1, o.retry, err)
			// check if we should retry
			if i < o.retry-1 {
				select {
				case <-o.gracefulStop:
					// Job was stopped, don't retry
					o.logger.Infof("%v job stopping during retry wait", o.id)
					return
				case <-time.After(o.calculateRetryDelay(i)):
					// Continue to next retry
					continue
				}
			}
		} else {
			o.logger.Infof("%v job completed successfully in %v", o.id, time.Since(startTime))
			break
		}
	}
	// Signal completion
	select {
	case o.gracefulStop <- struct{}{}:
	default:
		// Channel might be closed if Stop() was called
	}
}

// calculateRetryDelay returns a delay that increases with each retry attempt
func (o *Job) calculateRetryDelay(attempt int) time.Duration {
	// Simple linear backoff
	return o.retryDelay * time.Duration(attempt+1)
}

// Stop gracefully stops the job
func (o *Job) Stop() {
	o.logger.Infof("%v job stopping...", o.id)
	if !o.enabled {
		o.logger.Infof("%v job is disabled", o.id)
		close(o.gracefulStop)
		return
	}
	select {
	case <-o.gracefulStop:
		// Already stopped
	default:
		close(o.gracefulStop)
	}
	o.logger.Infof("%v job stopped", o.id)
}
