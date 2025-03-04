package cache

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"sync"
	"time"
	"walock"
	"walock/model"
)

type InmemoryCache struct {
	PersistProvider     walock.PersistProvider
	TccBusinessProvider walock.TccBusinessProvider
	QuotaLockWaitTime   prometheus.Histogram
	accounts            sync.Map
}

func (c *InmemoryCache) LoadAndLock(tx *gorm.DB, key string) (v model.LockerValue, err error) {
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
		newValue, err = c.PersistProvider.Load(tx, key)
		if err != nil {
			return
		}
		lock.Value = newValue
		log.Debug().Str("key", key).Msg("persist loaded")
	}
	return
}

func (c *InmemoryCache) Unlock(key string) {
	lock := c.ensureUserMiniLock(key)
	lock.Mu.Unlock()
}

func (c *InmemoryCache) DoTry(tx *gorm.DB, tccContext *model.TccContext, value model.LockerValue, tryBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("Dotry")
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

func (c *InmemoryCache) DoConfirm(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, confirmBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
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

func (c *InmemoryCache) DoCancel(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, cancelBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
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
func (c *InmemoryCache) ensureUserMiniLock(accountID string) *model.Locker {
	account, loaded := c.accounts.LoadOrStore(accountID, &model.Locker{})
	if !loaded {
		log.Debug().Str("userId", accountID).Msg("new account lock created")
	}

	return account.(*model.Locker)
}
