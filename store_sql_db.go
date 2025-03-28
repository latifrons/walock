package walock

import (
	"fmt"
	"github.com/latifrons/walock/consts"
	"github.com/latifrons/walock/model"
	"github.com/latifrons/walock/tcc"
	"github.com/rs/zerolog/log"
	"runtime/debug"
	"sync"
)

import (
	"gorm.io/gorm"
	"time"
)

// 考虑子事务屏障 https://zhuanlan.zhihu.com/p/388444465、
//空回滚，幂等，悬挂
//下面以TCC事务说明这些异常情况：
//空回滚：
//　　在没有调用 TCC 资源 Try 方法的情况下，调用了二阶段的 Cancel 方法。Cancel 方法需要识别出这是一个空回滚，然后直接返回成功。
//
//幂等：
//　　由于任何一个请求都可能出现网络异常，出现重复请求，所以所有的分布式事务分支，都需要保证幂等性
//
//悬挂：
//　　悬挂就是对于一个分布式事务，Try接口执行时，其二阶段 Cancel 接口已经在之前执行。Try方法需要识别出这是一个悬挂操作，然后直接返回失败。

// WalockStoreSqlDb 使用SQL作为持久化，存储WAL
// 适合中等压力场景
// TCC/WAL存储在数据库中
// 暂无Rotation

type WalockStoreSqlDb struct {
	DbRw               *gorm.DB
	Metrics            *model.Metrics
	BusinessProvider   BusinessProviderSql
	BarrierName        string
	BarrierDbTableName string

	tccBarrierSql tcc.TccBarrierSql
	accounts      sync.Map // string:*model.Locker
}

func (f *WalockStoreSqlDb) InitDefault() {
	f.tccBarrierSql = tcc.TccBarrierSql{
		BarrierName: f.BarrierName,
		DbTableName: f.BarrierDbTableName,
	}
}

// ensureUserMiniLock retrieves an existing account or creates a new one
func (f *WalockStoreSqlDb) ensureUserMiniLock(key model.LockerKey) *model.Locker {
	account, loaded := f.accounts.LoadOrStore(string(key), &model.Locker{})
	if !loaded {
		log.Debug().Str("userId", string(key)).Msg("new account lock created")
	}

	return account.(*model.Locker)
}

func (f *WalockStoreSqlDb) ensure(key model.LockerKey, tx *gorm.DB) (value model.LockerValue, err error) {
	value, err = f.BusinessProvider.LoadPersistedValue(tx, key)
	if err != nil {
		log.Error().Err(err).Msg("failed to load from persist")
		return
	}
	// replay wals
	err = f.BusinessProvider.CatchupWals(tx, key, value)
	if err != nil {
		log.Error().Err(err).Msg("failed to catchup wals")
		return
	}

	err = f.BusinessProvider.Flush(tx, value)
	if err != nil {
		return
	}
	return

}

func (f *WalockStoreSqlDb) LoadAndLock(tx *gorm.DB, key model.LockerKey) (lockValue model.LockerValue, err error) {
	startTime := time.Now()
	lock := f.ensureUserMiniLock(key)
	lock.Mu.Lock()
	lockedTime := time.Now()

	if f.Metrics.MetricsLockWaitTime != nil {
		f.Metrics.MetricsLockWaitTime.Observe(lockedTime.Sub(startTime).Seconds())
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
		newValue, err = f.ensure(key, tx)
		if err != nil {
			return
		}
		lock.Value = newValue
		log.Debug().Str("key", string(key)).Msg("persist loaded")
	}
	lockValue = lock.Value
	return
}

func (f *WalockStoreSqlDb) Unlock(key model.LockerKey) {
	lock := f.ensureUserMiniLock(key)
	lock.Mu.Unlock()
}

func (f *WalockStoreSqlDb) Traverse(fun func(key model.LockerKey, value model.LockerValue) bool) {
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

func (f *WalockStoreSqlDb) Keys() (keys []model.LockerKey) {
	keys = make([]model.LockerKey, 0)
	f.accounts.Range(func(key, value interface{}) bool {
		keys = append(keys, model.LockerKey(key.(string)))
		return true
	})
	return keys
}

func (f *WalockStoreSqlDb) Get(key model.LockerKey) (value model.LockerValue, err error) {

	valuePointer, err := f.LoadAndLock(f.DbRw, key)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		f.Metrics.LockHoldTime.WithLabelValues(f.Metrics.MetricsName + "_get").Observe(time.Now().Sub(startTime).Seconds())
		f.Unlock(key)
	}()

	value = valuePointer
	return
}

func (f *WalockStoreSqlDb) Must(tccContext *model.TccContext, lockKey model.LockerKey, mustBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	value, err := f.LoadAndLock(f.DbRw, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		f.Metrics.LockHoldTime.WithLabelValues(f.Metrics.MetricsName + "_must").Observe(time.Now().Sub(startTime).Seconds())
		f.Unlock(lockKey)
	}()

	exemptError := false // just to revert the transaction. do not return this error to caller

	err = f.DbRw.Transaction(func(tx *gorm.DB) error {
		var callIt bool
		callIt, err = f.tccBarrierSql.BarrierMust(tccContext, tx)
		if err != nil {
			return err
		}
		if !callIt {
			tccCode = consts.TccCode_Success
			message = "duplicate call"
			return nil
		}

		tccCode, code, message, err = f.DoMust(tx, tccContext, lockKey, value, mustBody)
		if err != nil {
			return err
		}
		if tccCode != consts.TccCode_Success {
			err = fmt.Errorf("must failed: code %s, msg %s", code, message)
			exemptError = true
			return err
		}

		return nil
	})
	if err != nil {
		log.Debug().Str("gid", tccContext.GlobalId).Str("bid", tccContext.BranchId).Err(err).Msg("tx reverted Must")
		if exemptError {
			// do not return this error to caller
			// this is just to revert the transaction
			err = nil
		}
		return
	}
	return

}

func (f *WalockStoreSqlDb) Try(tccContext *model.TccContext, lockKey model.LockerKey, tryBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	value, err := f.LoadAndLock(f.DbRw, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		f.Metrics.LockHoldTime.WithLabelValues(f.Metrics.MetricsName + "_try").Observe(time.Now().Sub(startTime).Seconds())
		f.Unlock(lockKey)
	}()

	exemptError := false // just to revert the transaction. do not return this error to caller

	err = f.DbRw.Transaction(func(tx *gorm.DB) error {
		var callIt bool
		callIt, err = f.tccBarrierSql.BarrierTry(tccContext, tx)
		if err != nil {
			return err
		}
		if !callIt {
			tccCode = consts.TccCode_Success
			message = "duplicate call"
			return nil
		}

		tccCode, code, message, err = f.DoTry(tx, tccContext, lockKey, value, tryBody)
		if err != nil {
			return err
		}
		if tccCode != consts.TccCode_Success {
			err = fmt.Errorf("try failed: code %s, msg %s", code, message)
			exemptError = true
			return err
		}
		return nil
	})
	if err != nil {
		log.Debug().Str("gid", tccContext.GlobalId).Str("bid", tccContext.BranchId).Err(err).Msg("tx reverted Try")
		if exemptError {
			// do not return this error to caller
			// this is just to revert the transaction
			err = nil
		}
		return
	}
	return
}

func (f *WalockStoreSqlDb) Confirm(tccContext *model.TccContext, lockKey model.LockerKey, confirmBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	exemptError := false // just to revert the transaction. do not return this error to caller

	err = f.DbRw.Transaction(func(tx *gorm.DB) error {
		var callIt bool
		callIt, err = f.tccBarrierSql.BarrierConfirm(tccContext, tx)
		if err != nil {
			return err
		}
		if !callIt {
			tccCode = consts.TccCode_Success
			message = "duplicate call"
			return nil
		}

		tccCode, code, message, err = f.DoConfirm(tx, tccContext, lockKey, nil, confirmBody)
		if err != nil {
			return err
		}
		if tccCode != consts.TccCode_Success {
			err = fmt.Errorf("confirm failed: code %s, msg %s", code, message)
			exemptError = true
			return err
		}
		return nil
	})
	if err != nil {
		log.Debug().Str("gid", tccContext.GlobalId).Str("bid", tccContext.BranchId).Err(err).Msg("tx reverted Confirm")
		if exemptError {
			// do not return this error to caller
			// this is just to revert the transaction
			err = nil
		}
		return
	}
	return
}

func (f *WalockStoreSqlDb) Cancel(tccContext *model.TccContext, lockKey model.LockerKey, cancelBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	value, err := f.LoadAndLock(f.DbRw, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		f.Metrics.LockHoldTime.WithLabelValues(f.Metrics.MetricsName + "_cancel").Observe(time.Now().Sub(startTime).Seconds())
		f.Unlock(lockKey)
	}()

	exemptError := false // just to revert the transaction. do not return this error to caller

	err = f.DbRw.Transaction(func(tx *gorm.DB) error {
		var callIt bool
		callIt, err = f.tccBarrierSql.BarrierCancel(tccContext, tx)
		if err != nil {
			return err
		}
		if !callIt {
			tccCode = consts.TccCode_Success
			message = "duplicate call"
			return nil
		}
		tccCode, code, message, err = f.DoCancel(tx, tccContext, lockKey, value, cancelBody)

		if err != nil {
			return err
		}
		if tccCode != consts.TccCode_Success {
			err = fmt.Errorf("cancel failed: code %s, msg %s", code, message)
			exemptError = true
			return err
		}

		return nil
	})
	if err != nil {
		log.Debug().Str("gid", tccContext.GlobalId).Str("bid", tccContext.BranchId).Err(err).Msg("tx reverted Cancel")
		if exemptError {
			// do not return this error to caller
			// this is just to revert the transaction
			err = nil
		}
		return
	}
	return
}

func (f *WalockStoreSqlDb) Update(lockKey model.LockerKey, updatedValue model.LockerValue,
	updater func(baseV, updateV model.LockerValue) (updated bool)) (err error) {
	baseValue, err := f.LoadAndLock(f.DbRw, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		f.Metrics.LockHoldTime.WithLabelValues(f.Metrics.MetricsName + "_update").Observe(time.Now().Sub(startTime).Seconds())
		f.Unlock(lockKey)
	}()

	updated := updater(baseValue, updatedValue)
	_ = updated
	return
}

func (f *WalockStoreSqlDb) FlushDirty() (err error) {
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
			err = f.BusinessProvider.Flush(f.DbRw, lock.Value)
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

	if f.Metrics.MetricsMapCount != nil {
		f.Metrics.MetricsMapCount.Set(float64(total))
	}

	return
}

func (f *WalockStoreSqlDb) DoMust(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, mustBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Trace().Str("tcc", tccContext.String()).Msg("DoMust")
	ok, code, message, mustWali, err := f.BusinessProvider.GenerateWalMust(tccContext, key, value, mustBody)
	if err != nil {
		return
	}
	if !ok {
		tccCode = consts.TccCode_Failed
		return
	}

	//write wal first
	err = f.BusinessProvider.FlushWal(tx, mustWali)
	if err != nil {
		return
	}

	// update memory
	err = f.BusinessProvider.ApplyWal(value, []interface{}{mustWali})
	if err != nil {
		log.Panic().Err(err).Msg("failed to apply mustWali")
		return
	}

	tccCode = consts.TccCode_Success
	return
}

func (f *WalockStoreSqlDb) DoTry(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, tryBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Trace().Str("tcc", tccContext.String()).Msg("DoTry")
	ok, code, message, tryWali, err := f.BusinessProvider.GenerateWalTry(tccContext, key, value, tryBody)
	if err != nil {
		return
	}
	if !ok {
		tccCode = consts.TccCode_Failed
		return
	}

	// write wal first
	err = f.BusinessProvider.FlushWal(tx, tryWali)
	if err != nil {
		return
	}

	// update memory
	err = f.BusinessProvider.ApplyWal(value, []interface{}{tryWali})
	if err != nil {
		log.Panic().Err(err).Msg("failed to apply tryWali")
		return
	}
	tccCode = consts.TccCode_Success
	return
}

func (f *WalockStoreSqlDb) DoConfirm(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, confirmBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Trace().Str("tcc", tccContext.String()).Msg("DoConfirm")

	// check if reserved resource is there.
	reservationWali, ok, code, message, err := f.BusinessProvider.LoadReservation(tx, tccContext)
	if err != nil {
		return
	}

	if !ok {
		tccCode = consts.TccCode_Failed
		return
	}

	confirmWali := f.BusinessProvider.GenerateWalConfirm(tccContext, key, value, reservationWali)
	if confirmWali == nil {
		tccCode = consts.TccCode_Success
		return
	}

	// write wal first
	err = f.BusinessProvider.FlushWal(tx, confirmWali)
	if err != nil {
		return
	}

	// update memory
	err = f.BusinessProvider.ApplyWal(value, []interface{}{confirmWali})
	if err != nil {
		log.Panic().Err(err).Msg("failed to apply confirmWali")
		return
	}
	tccCode = consts.TccCode_Success
	return
}

func (f *WalockStoreSqlDb) DoCancel(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, cancelBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	log.Trace().Str("tcc", tccContext.String()).Msg("DoCancel")

	// check if reserved resource is there.
	reservationWali, ok, code, message, err := f.BusinessProvider.LoadReservation(tx, tccContext)
	if !ok {
		tccCode = consts.TccCode_Failed
		return
	}

	revertWali := f.BusinessProvider.GenerateWalCancel(tccContext, key, value, reservationWali)
	if revertWali == nil {
		tccCode = consts.TccCode_Success
		return
	}

	// write wal first
	err = f.BusinessProvider.FlushWal(tx, revertWali)
	if err != nil {
		return
	}

	// update memory
	err = f.BusinessProvider.ApplyWal(value, []interface{}{revertWali})
	if err != nil {
		log.Panic().Err(err).Msg("failed to apply reservationWali")
		return
	}
	tccCode = consts.TccCode_Success
	return
}
