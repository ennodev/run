package run

type IJob interface {
	Id() string
	Enabled() bool
	Init() error
	Run()
	Stop()
}
