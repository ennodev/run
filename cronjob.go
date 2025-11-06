package run

import (
	gocontext "context"
	"time"
)

// CronJob implements a job that runs periodically
type CronJob struct {
	id           string
	enabled      bool
	runFunc      func(gocontext.Context) error
	initFunc     func(gocontext.Context) error
	delay        time.Duration
	timeout      time.Duration
	logger       ILogger
	gracefulStop chan struct{}
}

// CronJobOption defines functional options for configuring CronJob
type CronJobOption func(*CronJob)

// WithCronJobDelay sets the delay between job executions
func WithCronJobDelay(delay time.Duration) CronJobOption {
	return func(t *CronJob) {
		t.delay = delay
	}
}

// WithCronJobTimeout sets execution timeout for each job run
func WithCronJobTimeout(timeout time.Duration) CronJobOption {
	return func(t *CronJob) {
		t.timeout = timeout
	}
}

// NewCronJob creates a new instance of CronJob with options
func NewCronJob(
	id string,
	enabled bool,
	runFunc func(gocontext.Context) error,
	initFunc func(gocontext.Context) error,
	logger ILogger,
	options ...CronJobOption,
) IJob {
	job := &CronJob{
		logger:       logger,
		id:           id,
		enabled:      enabled,
		delay:        1 * time.Minute, // Default delay of 1 minute
		runFunc:      runFunc,
		initFunc:     initFunc,
		gracefulStop: make(chan struct{}),
		timeout:      5 * time.Minute, // Default timeout of 5 minutes
	}

	// Apply the optional configurations
	for _, option := range options {
		option(job)
	}

	// Set default delay if not specified
	if job.delay <= 0 {
		job.delay = time.Minute // Default to 1 minute
	}

	return job
}

// Id returns the job identifier
func (t *CronJob) Id() string {
	return t.id
}

// Enabled returns whether the job is enabled
func (t *CronJob) Enabled() bool {
	return t.enabled
}

func (t *CronJob) Init() error {
	if !t.enabled {
		t.logger.Infof("%v job is disabled", t.id)
		return nil
	}
	if t.initFunc == nil {
		t.logger.Infof("%v no initialization function provided, skipping init", t.id)
		return nil
	}
	t.logger.Infof("%v initializing job...", t.id)
	ctx, cancel := gocontext.WithTimeout(gocontext.Background(), t.timeout)
	defer cancel()
	if err := t.initFunc(ctx); err != nil {
		t.logger.Errorf("%v failed to initialize job: %v", t.id, err)
		return err
	}
	t.logger.Infof("%v initialized job", t.id)
	return nil
}

// Run executes the periodic job
func (t *CronJob) Run() {
	if !t.enabled {
		t.logger.Infof("%v job is disabled", t.id)
		return
	}

	t.logger.Infof("%v job started, delay: %v, timeout: %v", t.id, t.delay, t.timeout)
	defer t.logger.Infof("[Run] %v job stopped", t.id)

	// Create a ticker for periodic execution
	ticker := time.NewTicker(t.delay)
	defer ticker.Stop()

	// Execute immediately on start
	t.execute()

	// Main job loop
	for {
		select {
		case <-t.gracefulStop:
			return
		case <-ticker.C:
			t.execute()
			ticker.Reset(t.delay)
		}
	}
}

// execute runs a single execution of the job function with proper error handling
func (t *CronJob) execute() {
	// Handle panics to prevent job termination
	defer func() {
		if r := recover(); r != nil {
			t.logger.Errorf("%v job panicked: %v", t.id, r)
		}
	}()

	// Create timeout context for this execution
	execCtx, cancel := gocontext.WithTimeout(gocontext.Background(), t.timeout)
	defer cancel() // Always cancel to prevent context leak

	// Run the job function
	t.logger.Infof("%v job executing", t.id)
	startTime := time.Now()
	err := t.runFunc(execCtx)

	if err != nil {
		t.logger.Errorf("%v job execution failed: %v", t.id, err)
	} else {
		t.logger.Infof("%v job execution completed in %v", t.id, time.Since(startTime))
	}
}

// Stop gracefully stops the job
func (t *CronJob) Stop() {
	t.logger.Infof("%v job stopping...", t.id)
	select {
	case <-t.gracefulStop:
		// Already stopped
	default:
		close(t.gracefulStop)
	}
	t.logger.Infof("%v job stopped", t.id)
}
