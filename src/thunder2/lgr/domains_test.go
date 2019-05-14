package lgr

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func assertDomains(t *testing.T, domainsNames ...string) {
	var keys []string
	for k := range domains.domainMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sort.Strings(domainsNames)
	assert.Equal(t, domainsNames, keys)
}

func assertDomainLevels(domain *domainInfo, domainLevel int64, effectiveLevel int64,
	assert *assert.Assertions,
) {
	assert.Equal(domainLevel, domain.level, "bad level for %s", domain)
	assert.Equal(domain.getEffectiveLevel(), effectiveLevel)
}

func TestCreateDomain(t *testing.T) {
	clearLogDomains()
	domains.getOrCreateDomain("/foo/bar")
	SetLogLevel("/a/b", LvlInfo)
	assertDomains(t, "/", "/foo", "/foo/bar", "/a", "/a/b")
}

func TestInitLogDomains(t *testing.T) {
	clearLogDomains()
	assert := assert.New(t)
	doms, baseLgr := initLogDomains()
	assert.Equal(1, len(doms.domainMap), "Only one domain expected in new logDomains")
	_, exists := doms.domainMap["/"]
	assert.True(exists, "Base domain should exist in new log domains")
	assert.Equal(baseLgr.domain.canonicalName, "/")
}

func TestBaseLevels(t *testing.T) {
	clearLogDomains()
	assert := assert.New(t)
	rootDomain := domains.domainMap["/"]

	assertDomainLevels(rootDomain, LvlPassthrough, DefaultRootLevel, assert)

	SetLogLevel("/", LvlPassthrough)
	assertDomainLevels(rootDomain, LvlPassthrough, DefaultRootLevel, assert)

	for level := int64(0); level < LvlCount; level++ {
		SetLogLevel("/", level)
		assertDomainLevels(rootDomain, level, level, assert)
	}
}

func TestHierarchicalLevels(t *testing.T) {
	clearLogDomains()
	assert := assert.New(t)
	parentDomain := domains.getOrCreateDomain("/testDomain")
	childDomain1 := domains.getOrCreateDomain("/testDomain/Child1")
	childDomain2 := domains.getOrCreateDomain("/testDomain/Child2")

	// When no levels are set explicitly, effective level = DefaultRootLevel
	SetLogLevel(parentDomain.canonicalName, LvlPassthrough)
	assertDomainLevels(parentDomain, LvlPassthrough, DefaultRootLevel, assert)
	assertDomainLevels(childDomain1, LvlPassthrough, DefaultRootLevel, assert)
	assertDomainLevels(childDomain2, LvlPassthrough, DefaultRootLevel, assert)

	// Set child 2 level, shouldn't affect parent and child1
	SetLogLevel(childDomain2.canonicalName, LvlError)
	assertDomainLevels(parentDomain, LvlPassthrough, DefaultRootLevel, assert)
	assertDomainLevels(childDomain1, LvlPassthrough, DefaultRootLevel, assert)
	assertDomainLevels(childDomain2, LvlError, LvlError, assert)

	// Set parent level, should also affect child1 level
	SetLogLevel(parentDomain.canonicalName, LvlWarning)
	assertDomainLevels(parentDomain, LvlWarning, LvlWarning, assert)
	assertDomainLevels(childDomain1, LvlPassthrough, LvlWarning, assert)
	assertDomainLevels(childDomain2, LvlError, LvlError, assert)
}

func TestBadSetLevels(t *testing.T) {
	clearLogDomains()
	assert := assert.New(t)

	// negative checks
	err := SetLogLevel("/", -2)
	assert.Error(err, "no error setting bogus level")

	err = SetLogLevel("/", LvlCount)
	assert.Error(err, "no error setting bogus level")

	err = SetLogLevel("/", 500)
	assert.Error(err, "no error setting bogus level")

	err = SetLogLevel("foo", LvlInfo)
	assert.Error(err, "no error setting level for bad domain name")
}
