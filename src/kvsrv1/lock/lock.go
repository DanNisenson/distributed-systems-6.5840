package lock

import (
	"strconv"
	"strings"
	"sync"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

var (
	count  = 0
	mu     sync.Mutex
	logger = kvsrv.NewLogger()
)

func getId() string {
	mu.Lock()
	defer mu.Unlock()

	count += 1
	return strconv.Itoa(count)
}

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk

	lockname string
	id       string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}

	lk.lockname = lockname
	lk.id = getId()

	return lk
}

func (lk *Lock) Acquire() {
	logger.Info("Attempt acquire. lockId: %s", lk.id)
	ver := rpc.Tversion(0)

	for {
		val, ver2, _ := lk.ck.Get(lk.lockname)
		ver = ver2

		sliced := strings.Split(val, "-")
		val = ""
		locked := false
		if len(sliced) > 1 {
			val = sliced[1]
			if sliced[0] == "1" {
				locked = true
			}
		}

		logger.Info("Acquire?. val: %s. locekd: %t. lockId: %s", val, locked, lk.id)
		if val == "" || val == lk.id || !locked {

			err := lk.ck.Put(lk.lockname, "1-"+lk.id, ver)

			if err == rpc.OK {
				logger.Info("Acquired! lock. lockId: %s", lk.id)
				break
			} else {
				logger.Info("Acquire PUT failed. Retry acquire. err: %s. lockId: %s", err, lk.id)
			}
		} else {
			logger.Info("Acquire failed. Lock is active. Retry acquire. lockId: %s", lk.id)
		}
	}

}

func (lk *Lock) Release() {
	logger.Info("Attempt release. lockId: %s", lk.id)
	val, ver, err := lk.ck.Get(lk.lockname)

	sliced := strings.Split(val, "-")
	val = ""
	if len(sliced) > 1 {
		val = sliced[1]
	}

	if err == "ErrNoKey" {
		logger.Info("No key found for lock. LockId: %s", lk.id)
	} else if val != "" {
		err := lk.ck.Put(lk.lockname, "0-"+lk.id, ver)
		switch err {
		case rpc.OK:
			logger.Info("Released! lock. lockId: %s", lk.id)
		case rpc.ErrMaybe:
			logger.Info("Uncertain. Version tried: %d. Err: %s", ver, err)
		case rpc.ErrVersion:
			logger.Info("Outdated version trying to unlock. Version tried: %d. Err: %s", ver, err)
		}
	} else {
		logger.Info("Tried to release empty key. Version tried: %d", ver)
	}
}
