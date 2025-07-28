package run

import (
	"sync"
)

type Group struct {
	id           string
	enabled      bool
	jobs         []IJob
	gracefulStop chan struct{}
	logger       ILogger
}

func NewGroup(
	id string,
	enabled bool,
	jobs []IJob,
	logger ILogger,
) IJob {
	return &Group{
		id:           id,
		enabled:      enabled,
		jobs:         jobs,
		gracefulStop: make(chan struct{}),
		logger:       logger,
	}
}

func (m *Group) Id() string {
	return m.id
}

func (m *Group) Enabled() bool {
	return m.enabled
}

func (m *Group) Run() {
	if !m.enabled {
		m.logger.Infof("%v job is disabled", m.id)
		return
	}
	m.logger.Infof("%v job starting", m.id)
	defer m.logger.Infof("%v job stopped", m.id)

	// run jobs
	var wg sync.WaitGroup
	stopChan := make(chan struct{})
	for _, t := range m.jobs {
		if !t.Enabled() {
			m.logger.Infof("%v job is disabled", t.Id())
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Run()
		}()
	}
	go func() {
		wg.Wait()
		close(stopChan)
	}()
	// wait for all jobs to stop
	select {
	case <-stopChan:
		m.logger.Infof("all %v jobs stopped", m.id)
	case <-m.gracefulStop:
		m.logger.Infof("%v job stopping...", m.id)
		// stop all jobs
		m.stopJobs()
		wg.Wait()
	}
	// all jobs stopped, send signal to stop function
	m.gracefulStop <- struct{}{}
}

func (m *Group) Stop() {
	m.logger.Infof("[Stop] %v job stopping...", m.id)
	if !m.enabled {
		m.logger.Infof("%v job is disabled", m.id)
		close(m.gracefulStop)
		return
	}
	// send signal to stop
	m.gracefulStop <- struct{}{}
	// wait for all jobs to stop
	<-m.gracefulStop
	close(m.gracefulStop)
	m.logger.Infof("[Stop] %v job stopped", m.id)
}

func (m *Group) stopJobs() {
	m.logger.Infof("%v stopping %v jobs...", m.id, len(m.jobs))
	var wg sync.WaitGroup
	for _, t := range m.jobs {
		wg.Add(1)
		go func(t IJob) {
			defer wg.Done()
			t.Stop()
		}(t)
	}
	wg.Wait()
	m.logger.Infof("%v stopped %v jobs", m.id, len(m.jobs))
}
