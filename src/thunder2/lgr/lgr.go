package lgr

import (
	// Standard imports
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// Lgr is a struct that represents logging for a specific domain. It has 6 levels of severity.
// Log lines written using a Lgr will include severity, domain, tag, timestamp, filename, and
// message.
//
// A domain is a string that specifies a subsystem in the code, such as "/Accel/Client/Conn".  The
// severity of errors that are logged for a given domain can be set via lgr.SetLogLevel()
// 'context' is printed in each log line and contains domain name with tags (if set) in braces.
// For example. [/Accel/Client/Conn(10.3.1.1:12345)]
type Lgr struct {
	// Reference to domain instance in domains map
	domain  *domainInfo
	context string // domain annotated with tag, written in each log line
}

func newLgrInstance(domainName string, context string) *Lgr {
	return &Lgr{
		domain:  domains.getOrCreateDomain(domainName),
		context: context,
	}
}

// NewLgr creates a new logger. 'domain' should be full domain name like "/Accel/Client/Conn"
func NewLgr(domain string) *Lgr {
	return NewLgrT(domain, "")
}

// NewLgrT creates a new tagged logger.
// 'domain' should be full domain name like "/Accel/Server/Conn".
// Pass a non-emtpy tag which will be present in each line logged by this logger.
// Useful in cases like connection object where we can set tag to remote instances role.
// For example: [..../Conn(comm_10)/..]
func NewLgrT(domainName string, tag string) *Lgr {
	if domainName == "" || domainName == "/" {
		// All loggers should have some domain name for context.
		// Seeing /? in logs should prompt a fix.
		baseLogger.Warn("NewLgrT: empty/root domain not allowed. Using /?")
		domainName = "/?"
	}
	if domainName[0] != '/' {
		baseLogger.Warn("NewLgrT: domain name \"%s\" should start with '/'", domainName)
		domainName = "/" + domainName
	}
	context := domainName
	if tag != "" {
		context = fmt.Sprintf("%s(%s)", context, tag)
	}
	return newLgrInstance(domainName, context)
}

// NewChildLgr builds logger from given logger such that domain from parent logger is used as prefix
// for child logger's domain and parent's tag is used as is.
func (parent *Lgr) NewChildLgr(subDomainName string) *Lgr {
	return parent.NewChildLgrT(subDomainName, "")
}

// NewChildLgrT builds logger from given logger such that domain and tag from parent logger
// are used as prefix for child logger's domain and tag.
func (parent *Lgr) NewChildLgrT(subDomainName string, tag string) *Lgr {
	if subDomainName == "" {
		baseLogger.Error("empty sub-domain name to child logger . Using ???")
		subDomainName = "???"
	}
	fullDomainName := fmt.Sprint(parent.domain.canonicalName, "/", subDomainName)
	context := fmt.Sprint(parent.context, "/", subDomainName)
	if tag != "" {
		context = fmt.Sprintf("%s(%s)", context, tag)
	}
	return newLgrInstance(fullDomainName, context)
}

// output functions

func (lgr *Lgr) Trace(s string, args ...interface{}) {
	lgr.output(LvlTrace, s, args)
}

func (lgr *Lgr) Debug(s string, args ...interface{}) {
	lgr.output(LvlDebug, s, args)
}

func (lgr *Lgr) Info(s string, args ...interface{}) {
	lgr.output(LvlInfo, s, args)
}

func (lgr *Lgr) Warn(s string, args ...interface{}) {
	lgr.output(LvlWarning, s, args)
}

func (lgr *Lgr) Error(s string, args ...interface{}) {
	lgr.output(LvlError, s, args)
}

func (lgr *Lgr) Critical(s string, args ...interface{}) {
	lgr.output(LvlCritical, s, args)
}

func (lgr *Lgr) shouldOutput(level int64) bool {
	// if lgr.level == LvlWarn(1), and this is a Crit(3), return true
	return level >= lgr.domain.getEffectiveLevel()
}

func (lgr *Lgr) output(level int64, format string, args []interface{}) {
	// doing this to get the proper output order - logger's timestamp, then our severity,
	// domain/instance, filename/line number, then the message.
	if !lgr.shouldOutput(level) {
		return
	}

	var b strings.Builder
	b.Grow(130)
	b.WriteString("UTC ")
	b.WriteString(severity[level]) // "INFO"
	b.WriteString(": [")
	b.WriteString(lgr.context) // "/XX/YY(tag)/ZZ"
	b.WriteString("]: ")

	var ok bool
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "???"
		line = 0
	}
	short := filepath.Base(file)
	b.WriteString(short) // "filename.go"
	b.WriteString(":")
	b.WriteString(strconv.Itoa(line)) // "123"
	b.WriteString(": ")

	msg := fmt.Sprintf(format, args...) // caller's message
	b.WriteString(msg)
	outputString := b.String()
	logger.Output(0, outputString) // calldepth not used
}
