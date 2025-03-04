package cache

import (
	"context"
	"github.com/latifrons/walock"
	"github.com/latifrons/walock/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
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

func (c *InmemoryCache) LoadAndLock(ctx context.Context, tx *gorm.DB, key model.LockerKey) (v model.LockerValue, err error) {
	startTime := time.Now()
	lock := c.ensureUserMiniLock(key)
	lock.Mu.Lock()
	lockedTime := time.Now()

	if c.QuotaLockWaitTime != nil {
		c.QuotaLockWaitTime.Observe(lockedTime.Sub(startTime).Seconds())
	}

	defer func() {
		if err != nil {
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
	return
}

func (c *InmemoryCache) Unlock(key model.LockerKey) {
	lock := c.ensureUserMiniLock(key)
	lock.Mu.Unlock()
}

func (c *InmemoryCache) DoMust(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, mustBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("DoMust")
	ok, code, message, err := c.TccBusinessProvider.Must(tx, tccContext, key, mustBody)
	if err != nil {
		return
	}
	if !ok {
		tccCode = model.TccCode_Failed
		return
	}
	tccCode = model.TccCode_Success
	return
}

func (c *InmemoryCache) DoTry(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, tryBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("DoTry")
	ok, code, message, err := c.TccBusinessProvider.Try(tx, tccContext, value, tryBody)
	if err != nil {
		return
	}
	if !ok {
		tccCode = model.TccCode_Failed
		return
	}
	tccCode = model.TccCode_Success
	return
}

func (c *InmemoryCache) DoConfirm(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, confirmBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("DoConfirm")
	ok, code, message, err := c.TccBusinessProvider.Confirm(tx, tccContext, key, confirmBody)
	if err != nil {
		return
	}
	if !ok {
		tccCode = model.TccCode_Failed
		return
	}
	tccCode = model.TccCode_Success
	return
}

func (c *InmemoryCache) DoCancel(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, cancelBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("DoCancel")
	ok, code, message, err := c.TccBusinessProvider.Cancel(tx, tccContext, key, cancelBody)
	if err != nil {
		return
	}
	if !ok {
		tccCode = model.TccCode_Failed
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
	exists, lockedValue, err := c.PersistProvider.Load(tx, key)
	if err != nil {
		log.Error().Err(err).Msg("failed to load from persist")
		return
	}
	if !exists {
		lockedValue, err = c.LockerValueIniter.Create(ctx, key)
		if err != nil {
			log.Error().Err(err).Msg("failed to create new value")
			return
		}
	}
	// replay wals
	err = c.WalProvider.CatchupWals(tx, key, lockedValue)
	if err != nil {
		log.Error().Err(err).Msg("failed to catchup wals")
		return
	}
	return

}
