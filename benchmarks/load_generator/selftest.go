package load_generator

import (
	"errors"
	"fmt"
	"time"
)

// SelfTest exercises the display and errors.log without waiting for a real failure: one error of every kind, plus a
// status block that moves a few times under them. Both halves of the console can then be checked at a glance, and
// errors.log should end up with one line per error. It builds no cache and runs no load; the count it returns is how
// many errors it wrote.
func SelfTest(opts ...Option) (int64, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	con, errLog, _, err := o.openLogging()
	if err != nil {
		return 0, err
	}
	defer errLog.Close()

	errLog.opError("selftest", "get", "selftest:key-1", 0, errors.New("synthetic cache error"))
	errLog.badValue("selftest", "selftest:key-2", []byte("returned value"), []byte("expected value"))
	errLog.badKey("selftest", "selftest:key-3", []byte("returned value"))
	errLog.l2Error("selftest", "selftest:key-4", errors.New("synthetic l2 error"))
	errLog.cachePanic("selftest", "synthetic cache panic", false)
	errLog.panicked("selftest", "selftest", "synthetic worker panic")

	for tick := 1; tick <= 3; tick++ {
		for slot := range 3 {
			con.setStatus(slot, fmt.Sprintf("msg=selftest scenario=scenario-%d tick=%d", slot+1, tick))
		}
		errLog.opError(fmt.Sprintf("scenario-%d", tick), "set", "selftest:key-5", 128, errors.New("synthetic cache error"))
		time.Sleep(300 * time.Millisecond)
	}
	return errLog.count.Load(), nil
}
