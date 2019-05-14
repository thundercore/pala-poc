package lgr

import "strings"

// CreateLgrDomains is used to create a logger and a set of domains underneath it.
// It returns a LgrMap struct on which the various log functions can be called.
func CreateLgrDomains(domain string, children []string) LgrMap {
	parentlgr := NewLgr(domain)
	return buildLgrMap(parentlgr, children)
}

func buildLgrMap(parentLgr *Lgr, children []string) LgrMap {
	lmap := LgrMap{
		parent: parentLgr,
		m:      make(map[string]*Lgr),
	}
	for _, child := range children {
		key := strings.ToLower(child)
		lmap.m[key] = parentLgr.NewChildLgr(child)
	}
	return lmap
}

func CreateLgrDomainsT(domain string, tag string, children []string) LgrMap {
	parentlgr := NewLgrT(domain, tag)
	return buildLgrMap(parentlgr, children)
}

// A logger map holds a default parent and a set of child domains.  It is useful
// for clients that have many domains, but only want to keep track of one logger struct.
type LgrMap struct {
	parent *Lgr // The default root
	m      map[string]*Lgr
}

// GetLgr gets the logger for a child from a LgrMap.  If it doesn't exist,
// it returns the top level logger.
func (lmap *LgrMap) GetLgr(domain string) *Lgr {
	if lmap.parent == nil {
		return baseLogger
	}
	domain = strings.ToLower(domain)
	lgr, ok := lmap.m[domain]
	if !ok {
		return lmap.parent
	}
	return lgr
}

func (lmap *LgrMap) Trace(dom string, s string, args ...interface{}) {
	lmap.GetLgr(dom).output(LvlTrace, s, args)
}

func (lmap *LgrMap) Debug(dom string, s string, args ...interface{}) {
	lmap.GetLgr(dom).output(LvlDebug, s, args)
}

func (lmap *LgrMap) Info(dom string, s string, args ...interface{}) {
	lmap.GetLgr(dom).output(LvlInfo, s, args)
}

func (lmap *LgrMap) Warn(dom string, s string, args ...interface{}) {
	lmap.GetLgr(dom).output(LvlWarning, s, args)
}

func (lmap *LgrMap) Error(dom string, s string, args ...interface{}) {
	lmap.GetLgr(dom).output(LvlError, s, args)
}

func (lmap *LgrMap) Critical(dom string, s string, args ...interface{}) {
	lmap.GetLgr(dom).output(LvlCritical, s, args)
}
