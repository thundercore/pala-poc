package lgr

import (
	// Standard imports
	"bytes"
	"os"
	"testing"

	// Vendor imports
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	code := m.Run()
	clearLogDomains()
	os.Exit(code)
}

func setupTestLoggers() {
	SetLogLevel("/", LvlError)
	SetLogLevel("/A", LvlInfo)
}

// clearLogDomains clears the domain object and adds back root domain and logger
func clearLogDomains() {
	domains, baseLogger = initLogDomains()
}

func TestBadLgrNames(t *testing.T) {
	clearLogDomains()
	assert := assert.New(t)
	lgr := NewLgrT("", "")
	assert.Equal("/?", lgr.domain.canonicalName)

	lgr = NewLgrT("/", "")
	assert.Equal("/?", lgr.domain.canonicalName)

	lgr = NewLgrT("foo", "")
	assert.Equal("/foo", lgr.domain.canonicalName)

	childLgr := lgr.NewChildLgr("")
	assert.Equal(childLgr.domain.canonicalName, "/foo/???")
}

func TestOutputHandling(t *testing.T) {
	clearLogDomains()
	assert := assert.New(t)

	writer := new(bytes.Buffer)
	SetWriter(writer)
	setupTestLoggers()

	// baseLogger is set to level "error", see if it rejects lower level logs entries
	baseLogger.Info("info1")
	baseLogger.Warn("warn1")
	assert.Equal(0, writer.Len(), "stuff logged that shouldn't have been")

	// "/A" is set to level "info", so should log everything
	aLogger := NewLgr("/A")
	aLogger.Info("info1")
	currentLength := writer.Len()
	assert.NotZero(currentLength, "didn't log something that it should have")

	// create a child of "/A", make sure it logs stuff
	abLogger := aLogger.NewChildLgr("B")
	abLogger.Info("info1")
	assert.True(writer.Len() > currentLength, "didn't log something to /A/B that it should have")
	currentLength = writer.Len()

	// set "/A" to level "warning", make sure "/A/B" honors that level
	SetLogLevel("/A", LvlWarning)
	abLogger.Info("info2")
	assert.Equal(currentLength, writer.Len(), "logged when it shouldn't have")

	// but errors should still get logged
	abLogger.Error("error1")
	assert.True(writer.Len() > currentLength, "didn't log error to /A/B that it should have")
	currentLength = writer.Len()

	// set "/A" to passthrough, thus the root level "Error"
	SetLogLevel("/A", LvlPassthrough)
	abLogger.Warn("warn1")
	assert.Equal(currentLength, writer.Len(), "logged warning when it shouldn't have")
}

func TestChildLgrInstances(t *testing.T) {
	clearLogDomains()
	assert := assert.New(t)

	base1 := NewLgrT("/Parent", "base")
	child1 := base1.NewChildLgrT("Child", "c1")
	child2 := base1.NewChildLgrT("Child", "c2")

	assert.Equal("/Parent(base)/Child(c1)", child1.context, "child 1 bad context")
	assert.Equal("/Parent(base)/Child(c2)", child2.context, "child 2 bad context")
}

// TestLgrMap tests that loggers in the map have their domain correctly set.
func TestLgrMap(t *testing.T) {
	clearLogDomains()
	assert := assert.New(t)
	lgrMap := CreateLgrDomains("/X", []string{"Y", "Z"})
	assert.Equal(lgrMap.GetLgr("Y").domain, domains.domainMap["/x/y"])
	assert.Equal(lgrMap.GetLgr("Z").domain, domains.domainMap["/x/z"])

	SetLogLevel("/X/Y", LvlWarning)
	SetLogLevel("/X", LvlDebug)
	assertDomainLevels(lgrMap.parent.domain, LvlDebug, LvlDebug, assert)
	assertDomainLevels(lgrMap.GetLgr("Y").domain, LvlWarning, LvlWarning, assert)
	assertDomainLevels(lgrMap.GetLgr("Z").domain, LvlPassthrough, LvlDebug, assert)
}

func TestLogLevels(t *testing.T) {
	clearLogDomains()
	i := int64(0)
	for i < LvlCount {
		lvlName, _ := PrettyStringFromLevel(i)
		lvl, _ := LevelFromString(lvlName)
		assert.Equal(t, i, lvl)
		i++
	}
}
