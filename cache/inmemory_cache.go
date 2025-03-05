package cache

import (
	"context"
	"fmt"
	"github.com/latifrons/walock"
	"github.com/latifrons/walock/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"runtime/debug"
	"sync"
	"time"
)

type InmemoryCache struct {
	PersistProvider     walock.PersistProvider
	TccBusinessProvider walock.TccBusinessProvider
	WalProvider         walock.WalProvider
	LockerValueIniter   walock.LockValueIniter
	QuotaLockWaitTime   prometheus.Histogram
	accounts            sync.Map
}

func (c *InmemoryCache) LoadAndLock(ctx context.Context, tx *gorm.DB, key model.LockerKey) (lockValue model.LockerValue, err error) {
	startTime := time.Now()
	lock := c.ensureUserMiniLock(key)
	lock.Mu.Lock()
	lockedTime := time.Now()

	if c.QuotaLockWaitTime != nil {
		c.QuotaLockWaitTime.Observe(lockedTime.Sub(startTime).Seconds())
	}

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("stacktrace from panic: \n" + string(debug.Stack()))
			log.Error().Err(r.(error)).Stack().Msg("panic recovered")
			err = r.(error)
			lock.Mu.Unlock()
		} else if err != nil {
			lock.Mu.Unlock()
		}
	}()

	if lock.Value == nil {
		// load from database
		var newValue model.LockerValue
		newValue, err = c.ensure(ctx, tx, key)
		if err != nil {
			return
		}
		lock.Value = newValue
		log.Debug().Str("key", string(key)).Msg("persist loaded")
	}
	lockValue = lock.Value
	return
}

func (c *InmemoryCache) Unlock(key model.LockerKey) {
	lock := c.ensureUserMiniLock(key)
	lock.Mu.Unlock()
}

func (c *InmemoryCache) DoMust(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, mustBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("DoMust")
	ok, code, message, mustWali, err := c.TccBusinessProvider.MustWal(tx, tccContext, key, value, mustBody)
	if err != nil {
		return
	}
	if !ok {
		tccCode = model.TccCode_Failed
		return
	}

	// write wal first
	err = c.WalProvider.FlushWal(tx, mustWali)
	if err != nil {
		return
	}

	// update memory
	err = c.WalProvider.ApplyWal(value, []interface{}{mustWali})
	if err != nil {
		log.Panic().Err(err).Msg("failed to apply mustWali")
		return
	}

	tccCode = model.TccCode_Success
	return
}

func (c *InmemoryCache) DoTry(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, tryBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("DoTry")
	ok, code, message, tryWali, err := c.TccBusinessProvider.TryWal(tx, tccContext, key, value, tryBody)
	if err != nil {
		return
	}
	if !ok {
		tccCode = model.TccCode_Failed
		return
	}

	// write wal first
	err = c.WalProvider.FlushWal(tx, tryWali)
	if err != nil {
		return
	}

	// update memory
	err = c.WalProvider.ApplyWal(value, []interface{}{tryWali})
	if err != nil {
		log.Panic().Err(err).Msg("failed to apply tryWali")
		return
	}
	tccCode = model.TccCode_Success
	return
}

func (c *InmemoryCache) DoConfirm(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, confirmBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("DoConfirm")

	// check if reserved resource is there.
	reservationWali, ok, code, message, err := c.TccBusinessProvider.LoadReservation(tx, tccContext)
	if err != nil {
		return
	}

	if !ok {
		tccCode = model.TccCode_Failed
		return
	}

	confirmWali := c.TccBusinessProvider.ConfirmWal(tx, tccContext, key, value, reservationWali)
	if confirmWali == nil {
		tccCode = model.TccCode_Success
		return
	}

	// write wal first
	err = c.WalProvider.FlushWal(tx, confirmWali)
	if err != nil {
		return
	}

	// update memory
	err = c.WalProvider.ApplyWal(value, []interface{}{confirmWali})
	if err != nil {
		log.Panic().Err(err).Msg("failed to apply confirmWali")
		return
	}
	tccCode = model.TccCode_Success
	return
}

func (c *InmemoryCache) DoCancel(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, cancelBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("DoCancel")

	// check if reserved resource is there.
	reservationWali, ok, code, message, err := c.TccBusinessProvider.LoadReservation(tx, tccContext)
	if !ok {
		tccCode = model.TccCode_Failed
		return
	}

	revertWali := c.TccBusinessProvider.CancelWal(tx, tccContext, key, value, reservationWali)
	if revertWali == nil {
		tccCode = model.TccCode_Success
		return
	}

	// write wal first
	err = c.WalProvider.FlushWal(tx, revertWali)
	if err != nil {
		return
	}

	// update memory
	err = c.WalProvider.ApplyWal(value, []interface{}{revertWali})
	if err != nil {
		log.Panic().Err(err).Msg("failed to apply reservationWali")
		return
	}
	tccCode = model.TccCode_Success
	return
}

// ensureUserMiniLock retrieves an existing account or creates a new one
func (c *InmemoryCache) ensureUserMiniLock(key model.LockerKey) *model.Locker {
	account, loaded := c.accounts.LoadOrStore(string(key), &model.Locker{})
	if !loaded {
		log.Debug().Str("userId", string(key)).Msg("new account lock created")
	}

	return account.(*model.Locker)
}

func (c *InmemoryCache) ensure(ctx context.Context, tx *gorm.DB, key model.LockerKey) (value model.LockerValue, err error) {
	exists, value, err := c.PersistProvider.Load(tx, key)
	if err != nil {
		log.Error().Err(err).Msg("failed to load from persist")
		return
	}
	if !exists {
		value, err = c.LockerValueIniter.Create(ctx, key)
		if err != nil {
			log.Error().Err(err).Msg("failed to create new value")
			return
		}
	}
	// replay wals
	err = c.WalProvider.CatchupWals(tx, key, value)
	if err != nil {
		log.Error().Err(err).Msg("failed to catchup wals")
		return
	}
	return

}
