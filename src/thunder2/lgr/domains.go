package lgr

import (
	// Standard imports
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	// Thunder imports
)

type domainInfo struct {
	parent        *domainInfo
	level         int64
	canonicalName string
}

func (dI *domainInfo) getEffectiveLevel() int64 {
	lgrLevel := atomic.LoadInt64(&dI.level)
	// Root logger's level is never LvlPassthrough
	if lgrLevel == LvlPassthrough {
		if dI.parent != nil {
			lgrLevel = dI.parent.getEffectiveLevel()
		} else {
			lgrLevel = DefaultRootLevel
		}
	}
	return lgrLevel
}

type logDomains struct {
	sync.Mutex
	domainMap map[string]*domainInfo
}

func (doms *logDomains) setLevel(fullDomain string, level int64) error {
	domains.Lock()
	defer domains.Unlock()
	if level != LvlPassthrough && (level < 0 || level >= LvlCount) {
		// bad level
		return fmt.Errorf("setLevel %s: bad level %d", fullDomain, level)
	}
	if fullDomain[0] != '/' {
		return fmt.Errorf("setLevel %s: domain must start with '/'", fullDomain)
	}
	key := strings.ToLower(fullDomain)
	// If setting logging level before creating any loggers for the domain
	if domInfo, exists := doms.domainMap[key]; !exists {
		domInfo := doms.getOrCreateDomainLocked(fullDomain)
		atomic.StoreInt64(&domInfo.level, level)
	} else {
		atomic.StoreInt64(&domInfo.level, level)
	}
	return nil
}

// getOrCreateDomain gets domainInfo for the given name. Creates new one if not already present.
// Also creates any missing ancestors between given domainName and root.
// Assumes that domainName starts with "/".
func (doms *logDomains) getOrCreateDomain(domainName string) *domainInfo {
	doms.Lock()
	defer doms.Unlock()
	return doms.getOrCreateDomainLocked(domainName)
}

// getOrCreateDomainLocked is same as getOrCreateDomain except that it assumes that lock is held.
// Needed since this function is recursive in nature, whereas lock can be acquired only once.
func (doms *logDomains) getOrCreateDomainLocked(domainName string) *domainInfo {
	key := strings.ToLower(domainName)
	domainI, exists := doms.domainMap[key]
	if !exists {
		last := strings.LastIndex(domainName, "/")
		var parentDomain *domainInfo
		if last > 0 { // If parent of domainName in not root. For eg. /abc/def
			parentDomain = doms.getOrCreateDomainLocked(domainName[0:last])
		} else {
			parentDomain = doms.domainMap["/"] // created by initLogDomains()
		}
		domainI = &domainInfo{
			parent:        parentDomain,
			level:         LvlPassthrough,
			canonicalName: domainName,
		}
		doms.domainMap[key] = domainI
	}
	return domainI
}

// SetLogLevel sets the log level for a particular log domain and its children.  Pass LvlWarning for
// example to output everything at warning and above. pass LvlPassthrough to set it to use the
// settings of the parent level.  If a child of the specified level has its own level set, that
// domain's log level will not be changed by this
func SetLogLevel(fullDomain string, level int64) error {
	return domains.setLevel(fullDomain, level)
}

func GetLogLevel(fullDomain string) (int64, error) {
	key := strings.ToLower(fullDomain)
	domainI, exists := domains.domainMap[key]
	if exists == false {
		return -1, errors.New("non-existant key")
	}
	lvl := atomic.LoadInt64(&domainI.level)
	return lvl, nil
}
