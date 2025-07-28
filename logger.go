package run

type ILogger interface {
	Infof(template string, args ...interface{})
	Debugf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Fatalf(template string, args ...interface{})
	Info(args ...interface{})
	Debug(args ...interface{})
	Error(args ...interface{})
	Warn(args ...interface{})
	Fatal(args ...interface{})
}
