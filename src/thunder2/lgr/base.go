package lgr

import (
	"io"
	"log"
	"os"
)

const (
	DefaultRootLevel = LvlInfo

	// Logging format for Go's log implementation
	flags = log.Ldate | log.Ltime | log.LUTC | log.Lmicroseconds
)

// Default logging level of the framework. Used when root logger is set to passthrough.

var (
	domains, baseLogger = initLogDomains()

	// logger's output stream can be changed via SetWriter()
	logger              = log.New(os.Stderr, "", flags)
	outWriter io.Writer = os.Stderr
)

func initLogDomains() (*logDomains, *Lgr) {
	doms := &logDomains{
		domainMap: make(map[string]*domainInfo),
	}
	// Instantiate root level in logger hierarchy.
	rootDomainInfo := &domainInfo{
		parent:        nil,
		level:         LvlPassthrough,
		canonicalName: "/",
	}
	doms.domainMap["/"] = rootDomainInfo
	rootLogger := &Lgr{
		domain:  rootDomainInfo,
		context: "/",
	}
	return doms, rootLogger // only place where we create "/" logger
}

// SetWriter allows the io.Writer used for writing to be set.  The default is stderr
func SetWriter(writer io.Writer) {
	logger.SetOutput(writer)
	outWriter = writer
}
