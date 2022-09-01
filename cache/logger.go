package cache

const (
	logLevelFatal int = iota
	logLevelSystem
	logLevelError
	logLevelWarning
	logLevelInfo
	logLevelDebug
)

type logger interface {
	Fatal(string, ...string)
	System(string, ...string)
	Error(string, ...string)
	Warning(string, ...string)
	Info(string, ...string)
	Debug(string, ...string)
}

type loggerWrapper struct {
	logger
}

func (l loggerWrapper) dispatch(logLevel int, source string, message string) {
	if l.logger != nil {
		switch logLevel {
		case logLevelFatal:
			l.Fatal(source, message)
		case logLevelSystem:
			l.System(source, message)
		case logLevelError:
			l.Error(source, message)
		case logLevelWarning:
			l.Warning(source, message)
		case logLevelInfo:
			l.Info(source, message)
		case logLevelDebug:
			l.Debug(source, message)
		}
	}
}
