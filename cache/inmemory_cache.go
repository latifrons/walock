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
	MetricsLockWaitTime prometheus.Histogram
	MetricsMapCount     prometheus.Gauge
	accounts            sync.Map // string:*model.Locker
}

func (f *InmemoryCache) Traverse(fun func(key model.LockerKey, value model.LockerValue) bool) {
	total := 0

	f.accounts.Range(func(key, value any) bool {
		total += 1
		lock := f.ensureUserMiniLock(model.LockerKey(key.(string)))
		lock.Mu.Lock()

		defer func() {
			lock.Mu.Unlock()
		}()

		if lock.Value == nil {
			log.Warn().Any("key", key).Msg("for some reason value is nil. maybe it is being initialized")
			return true
		}

		return fun(model.LockerKey(key.(string)), lock.Value)
	})
}

func (f *InmemoryCache) Keys() []model.LockerKey {
	keys := make([]model.LockerKey, 0)
	f.accounts.Range(func(key, value interface{}) bool {
		keys = append(keys, model.LockerKey(key.(string)))
		return true
	})
	return keys
}

func (f *InmemoryCache) LoadAndLock(ctx context.Context, tx *gorm.DB, key model.LockerKey) (lockValue model.LockerValue, err error) {
	startTime := time.Now()
	lock := f.ensureUserMiniLock(key)
	lock.Mu.Lock()
	lockedTime := time.Now()

	if f.MetricsLockWaitTime != nil {
		f.MetricsLockWaitTime.Observe(lockedTime.Sub(startTime).Seconds())
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
		newValue, err = f.ensure(ctx, tx, key)
		if err != nil {
			return
		}
		lock.Value = newValue
		log.Debug().Str("key", string(key)).Msg("persist loaded")
	}
	lockValue = lock.Value
	return
}

func (f *InmemoryCache) Unlock(key model.LockerKey) {
	lock := f.ensureUserMiniLock(key)
	lock.Mu.Unlock()
}

func (f *InmemoryCache) DoMust(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, mustBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("DoMust")
	ok, code, message, mustWali, err := f.TccBusinessProvider.MustWal(tx, tccContext, key, value, mustBody)
	if err != nil {
		return
	}
	if !ok {
		tccCode = model.TccCode_Failed
		return
	}

	// write wal first
	err = f.WalProvider.FlushWal(tx, mustWali)
	if err != nil {
		return
	}

	// update memory
	err = f.WalProvider.ApplyWal(value, []interface{}{mustWali})
	if err != nil {
		log.Panic().Err(err).Msg("failed to apply mustWali")
		return
	}

	tccCode = model.TccCode_Success
	return
}

func (f *InmemoryCache) DoTry(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, tryBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("DoTry")
	ok, code, message, tryWali, err := f.TccBusinessProvider.TryWal(tx, tccContext, key, value, tryBody)
	if err != nil {
		return
	}
	if !ok {
		tccCode = model.TccCode_Failed
		return
	}

	// write wal first
	err = f.WalProvider.FlushWal(tx, tryWali)
	if err != nil {
		return
	}

	// update memory
	err = f.WalProvider.ApplyWal(value, []interface{}{tryWali})
	if err != nil {
		log.Panic().Err(err).Msg("failed to apply tryWali")
		return
	}
	tccCode = model.TccCode_Success
	return
}

func (f *InmemoryCache) DoConfirm(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, confirmBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("DoConfirm")

	// check if reserved resource is there.
	reservationWali, ok, code, message, err := f.TccBusinessProvider.LoadReservation(tx, tccContext)
	if err != nil {
		return
	}

	if !ok {
		tccCode = model.TccCode_Failed
		return
	}

	confirmWali := f.TccBusinessProvider.ConfirmWal(tx, tccContext, key, value, reservationWali)
	if confirmWali == nil {
		tccCode = model.TccCode_Success
		return
	}

	// write wal first
	err = f.WalProvider.FlushWal(tx, confirmWali)
	if err != nil {
		return
	}

	// update memory
	err = f.WalProvider.ApplyWal(value, []interface{}{confirmWali})
	if err != nil {
		log.Panic().Err(err).Msg("failed to apply confirmWali")
		return
	}
	tccCode = model.TccCode_Success
	return
}

func (f *InmemoryCache) DoCancel(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, cancelBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Debug().Str("tcc", tccContext.String()).Msg("DoCancel")

	// check if reserved resource is there.
	reservationWali, ok, code, message, err := f.TccBusinessProvider.LoadReservation(tx, tccContext)
	if !ok {
		tccCode = model.TccCode_Failed
		return
	}

	revertWali := f.TccBusinessProvider.CancelWal(tx, tccContext, key, value, reservationWali)
	if revertWali == nil {
		tccCode = model.TccCode_Success
		return
	}

	// write wal first
	err = f.WalProvider.FlushWal(tx, revertWali)
	if err != nil {
		return
	}

	// update memory
	err = f.WalProvider.ApplyWal(value, []interface{}{revertWali})
	if err != nil {
		log.Panic().Err(err).Msg("failed to apply reservationWali")
		return
	}
	tccCode = model.TccCode_Success
	return
}

// ensureUserMiniLock retrieves an existing account or creates a new one
func (f *InmemoryCache) ensureUserMiniLock(key model.LockerKey) *model.Locker {
	account, loaded := f.accounts.LoadOrStore(string(key), &model.Locker{})
	if !loaded {
		log.Debug().Str("userId", string(key)).Msg("new account lock created")
	}

	return account.(*model.Locker)
}

func (f *InmemoryCache) ensure(ctx context.Context, tx *gorm.DB, key model.LockerKey) (value model.LockerValue, err error) {
	value, err = f.PersistProvider.Load(tx, key)
	if err != nil {
		log.Error().Err(err).Msg("failed to load from persist")
		return
	}
	// replay wals
	err = f.WalProvider.CatchupWals(tx, key, value)
	if err != nil {
		log.Error().Err(err).Msg("failed to catchup wals")
		return
	}

	err = f.PersistProvider.Flush(tx, value)
	if err != nil {
		return
	}
	return

}

func (f *InmemoryCache) FlushDirty(tx *gorm.DB) (err error) {
	refreshCount := 0

	total := 0

	f.accounts.Range(func(key, value interface{}) bool {
		total += 1
		lock := f.ensureUserMiniLock(model.LockerKey(key.(string)))
		lock.Mu.Lock()

		defer func() {
			lock.Mu.Unlock()
		}()

		if lock.Value == nil {
			log.Warn().Any("key", key).Msg("for some reason value is nil. maybe it is being initialized")
			return true
		}

		if lock.Value.IsDirty() || lock.Value.GetDbVersion() != lock.Value.GetVersion() {
			err = f.PersistProvider.Flush(tx, lock.Value)
			if err != nil {
				log.Error().Err(err).Msg("failed to flush back")
				return false
			}
			lock.Value.SetDbVersion(lock.Value.GetVersion())
			lock.Value.SetDirty(false)

			refreshCount++
		}

		return true
	})
	log.Info().Int("mapSize", total).Int("refreshCount", refreshCount).Msg("flushing back")

	if f.MetricsMapCount != nil {
		f.MetricsMapCount.Set(float64(total))
	}

	return
}
