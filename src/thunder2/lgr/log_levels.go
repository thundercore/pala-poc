package lgr

import (
	"fmt"
	"strings"
)

const (
	// Constants that define the log levels.  These constants are ordered by severity, and are
	// used as indices into an array, so they must be 0-based and consecutive.  LvlPassthrough
	// is not used as an array index.
	LvlTrace int64 = iota
	LvlDebug
	LvlInfo
	LvlWarning
	LvlError
	LvlCritical
	LvlCount                  // not a real level; used to count how many levels there are
	LvlPassthrough int64 = -1 // use the log level of the parent Lgr
)

// String used in log files to denote level.
// These are fixed to one common length to have easy visual parsing of logfile.
var severity = []string{ // indexed by the Lvl... constants
	"TRACE", "DEBUG", "INFO ", "WARN ", "ERROR", "CRIT ",
}

// LevelFromString is used by config framework to get int level from string (passed as command arg)
// when debugCli's set/setmany command is used to set log level.
// Since it's rarely used, we iterate over array instead of having another map just for lookup.
func LevelFromString(name string) (int64, error) {
	nameUpper := strings.ToUpper(name)
	for level, n := range severity {
		if strings.TrimSpace(n) == nameUpper {
			return int64(level), nil
		}
	}
	return 0, fmt.Errorf("Bad level: %s", name)
}

// PrettyStringFromLevel is used by config framework to get string level from int
// when debugCli's view command is used.
// Since it's rarely used, we iterate over array instead of having another map just for lookup.
func PrettyStringFromLevel(level int64) (string, error) {
	if level == LvlPassthrough {
		return "PASSTHROUGH", nil
	} else if level >= 0 && level < LvlCount {
		return strings.TrimSpace(severity[level]), nil
	} else {
		return "", fmt.Errorf("Bad level: %d", level)
	}
}
