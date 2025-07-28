package run

type IJob interface {
	Id() string
	Enabled() bool
	Run()
	Stop()
}
