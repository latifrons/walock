package walock

import (
	"fmt"
	"github.com/latifrons/walock/consts"
	"github.com/latifrons/walock/model"
	"github.com/latifrons/walock/tcc"
	"github.com/rs/zerolog/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"runtime/debug"
	"sync"
)

import (
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

// WalockStoreLevelDb 使用本地LevelDB/RocksDB作为持久化，存储WAL
// 适合高压力场景，以及对性能要求高的场景
// TCC/WAL存储在本地磁盘中，因此需要有如下保证：
// lockKey用于对TCC事务的持久化进行加锁，由于levelDB本身没有事务，因此TCC中的GlobalID必须与lockKey绑定。不可以出现两个lockKey使用同一个GlobalID的情况。
// 本地数据库会有rotation，以防止数据过大，导致性能下降。
// 一般一天清理一次。为了避免遗留事务，上一个周期的数据库不会被立即清理。
// 当需要WAL重放时，将会从本周期和上一个周期的数据库中分别进行重放。

type WalockStoreLevelDb struct {
	DbRw               *leveldb.DB
	Metrics            *model.Metrics
	BusinessProvider   BusinessProviderLevelDb
	TccBarrierLevelDb  tcc.TccBarrierLevelDb
	BarrierName        string
	BarrierDbTableName string
	WriteOption        *opt.WriteOptions

	accounts sync.Map // string:*model.Locker
}

func (f *WalockStoreLevelDb) InitDefault() {
}

// ensureUserMiniLock retrieves an existing account or creates a new one
func (f *WalockStoreLevelDb) ensureUserMiniLock(key model.LockerKey) *model.Locker {
	account, loaded := f.accounts.LoadOrStore(string(key), &model.Locker{})
	if !loaded {
		log.Debug().Str("userId", string(key)).Msg("new account lock created")
	}

	return account.(*model.Locker)
}

func (f *WalockStoreLevelDb) ensure(key model.LockerKey, tx *leveldb.DB) (value model.LockerValue, err error) {
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

func (f *WalockStoreLevelDb) LoadAndLock(tx *leveldb.DB, key model.LockerKey) (lockValue model.LockerValue, err error) {
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

func (f *WalockStoreLevelDb) Unlock(key model.LockerKey) {
	lock := f.ensureUserMiniLock(key)
	lock.Mu.Unlock()
}

func (f *WalockStoreLevelDb) Traverse(fun func(key model.LockerKey, value model.LockerValue) bool) {
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

func (f *WalockStoreLevelDb) Keys() (keys []model.LockerKey) {
	keys = make([]model.LockerKey, 0)
	f.accounts.Range(func(key, value interface{}) bool {
		keys = append(keys, model.LockerKey(key.(string)))
		return true
	})
	return keys
}

func (f *WalockStoreLevelDb) Get(key model.LockerKey) (value model.LockerValue, err error) {

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

func (f *WalockStoreLevelDb) Must(tccContext *model.TccContext, lockKey model.LockerKey, mustBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	value, err := f.LoadAndLock(f.DbRw, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		f.Metrics.LockHoldTime.WithLabelValues(f.Metrics.MetricsName + "_must").Observe(time.Now().Sub(startTime).Seconds())
		f.Unlock(lockKey)
	}()

	v := tcc.BuildTccBarrierReceiver(f.BarrierName, tccContext.GlobalId, tccContext.BranchId, consts.TccBranchTypeMust)

	// check TCC
	{
		var callIt bool
		callIt, err = f.TccBarrierLevelDb.CheckBarrierMust([]byte(v.Key), f.DbRw)
		if err != nil {
			return
		}
		if !callIt {
			tccCode = consts.TccCode_Success
			message = "duplicate call"
			return
		}
	}

	// generate wal
	var mustWal []byte
	{
		var ok bool
		ok, code, message, mustWal, err = f.BusinessProvider.GenerateWalMust(tccContext, lockKey, value, mustBody)
		if err != nil {
			return
		}
		if !ok {
			tccCode = consts.TccCode_Failed
			return
		}
	}

	// write tcc and mustWal in one transaction
	{
		b := &leveldb.Batch{}
		b.Put([]byte(v.Key), []byte{})

		waliKey := fmt.Sprintf("wali_%s_%d", lockKey, value.GetVersion()+1)
		b.Put([]byte(waliKey), mustWal)

		// write wal first
		err = f.DbRw.Write(b, f.WriteOption)
		if err != nil {
			log.Error().Err(err).Str("tcc", tccContext.String()).Msg("failed to write wal")
			return
		}
	}

	// update memory. this must success, or we will have a dirty wal
	f.BusinessProvider.MustApplyWal(value, []WalBytes{mustWal})
	tccCode = consts.TccCode_Success
	return

}

func (f *WalockStoreLevelDb) Try(tccContext *model.TccContext, lockKey model.LockerKey, tryBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	value, err := f.LoadAndLock(f.DbRw, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		f.Metrics.LockHoldTime.WithLabelValues(f.Metrics.MetricsName + "_try").Observe(time.Now().Sub(startTime).Seconds())
		f.Unlock(lockKey)
	}()

	v := tcc.BuildTccBarrierReceiver(f.BarrierName, tccContext.GlobalId, tccContext.BranchId, consts.TccBranchTypeTry)

	// check TCC
	{
		var callIt bool
		callIt, err = f.TccBarrierLevelDb.CheckBarrierTry([]byte(v.Key), f.DbRw)
		if err != nil {
			return
		}
		if !callIt {
			tccCode = consts.TccCode_Success
			message = "duplicate call"
			return
		}
	}

	// generate wal
	var tryWal []byte
	{
		var ok bool
		ok, code, message, tryWal, err = f.BusinessProvider.GenerateWalTry(tccContext, lockKey, value, tryBody)
		if err != nil {
			return
		}
		if !ok {
			tccCode = consts.TccCode_Failed
			return
		}
	}

	// write tcc and mustWali in one transaction
	{
		b := &leveldb.Batch{}
		b.Put([]byte(v.Key), []byte{})

		waliKey := fmt.Sprintf("wali_%s_%d", lockKey, value.GetVersion()+1)
		b.Put([]byte(waliKey), tryWal)

		// write wal first
		err = f.DbRw.Write(b, f.WriteOption)
		if err != nil {
			log.Error().Err(err).Str("tcc", tccContext.String()).Msg("failed to write wal")
			return
		}
	}

	// update memory. this must success, or we will have a dirty wal
	f.BusinessProvider.MustApplyWal(value, []WalBytes{tryWal})
	tccCode = consts.TccCode_Success

	return
}

func (f *WalockStoreLevelDb) Confirm(tccContext *model.TccContext, lockKey model.LockerKey, confirmBody interface{}) (tccCode model.TccCode, code string, message string, err error) {

	value, err := f.LoadAndLock(f.DbRw, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		f.Metrics.LockHoldTime.WithLabelValues(f.Metrics.MetricsName + "_must").Observe(time.Now().Sub(startTime).Seconds())
		f.Unlock(lockKey)
	}()

	v := tcc.BuildTccBarrierReceiver(f.BarrierName, tccContext.GlobalId, tccContext.BranchId, consts.TccBranchTypeConfirm)

	// check TCC
	{
		var callIt bool
		callIt, err = f.TccBarrierLevelDb.CheckBarrierConfirm([]byte(v.Key), f.DbRw)
		if err != nil {
			return
		}
		if !callIt {
			tccCode = consts.TccCode_Success
			message = "duplicate call"
			return
		}
	}

	// get reservationWal
	var reservationWal WalBytes
	{
		var ok bool
		reservationWal, ok, code, message, err = f.BusinessProvider.LoadReservation(f.DbRw, tccContext)
		if err != nil {
			return
		}
		if !ok {
			tccCode = consts.TccCode_Failed
			return
		}
	}

	// generate wal
	var confirmWal []byte
	{
		confirmWal = f.BusinessProvider.GenerateWalConfirm(tccContext, lockKey, value, reservationWal)
	}
	// write tcc and mustWal in one transaction
	{
		b := &leveldb.Batch{}
		b.Put([]byte(v.Key), []byte{})

		waliKey := fmt.Sprintf("wali_%s_%d", lockKey, value.GetVersion()+1)
		b.Put([]byte(waliKey), confirmWal)

		// write wal first
		err = f.DbRw.Write(b, f.WriteOption)
		if err != nil {
			log.Error().Err(err).Str("tcc", tccContext.String()).Msg("failed to write wal")
			return
		}
	}

	// update memory. this must success, or we will have a dirty wal
	f.BusinessProvider.MustApplyWal(value, []WalBytes{confirmWal})
	tccCode = consts.TccCode_Success
	return
}

func (f *WalockStoreLevelDb) Cancel(tccContext *model.TccContext, lockKey model.LockerKey, cancelBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	value, err := f.LoadAndLock(f.DbRw, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		f.Metrics.LockHoldTime.WithLabelValues(f.Metrics.MetricsName + "_cancel").Observe(time.Now().Sub(startTime).Seconds())
		f.Unlock(lockKey)
	}()

	vTry := tcc.BuildTccBarrierReceiver(f.BarrierName, tccContext.GlobalId, tccContext.BranchId, consts.TccBranchTypeTry)
	vCancel := tcc.BuildTccBarrierReceiver(f.BarrierName, tccContext.GlobalId, tccContext.BranchId, consts.TccBranchTypeCancel)

	// check TCC
	{
		var callIt bool
		callIt, err = f.TccBarrierLevelDb.CheckBarrierCancel([]byte(vTry.Key), []byte(vCancel.Key), f.DbRw)
		if err != nil {
			return
		}
		if !callIt {
			tccCode = consts.TccCode_Success
			message = "duplicate call"
			return
		}
	}
	// get reservationWal
	var reservationWal WalBytes
	{
		var ok bool
		reservationWal, ok, code, message, err = f.BusinessProvider.LoadReservation(f.DbRw, tccContext)
		if err != nil {
			return
		}
		if !ok {
			tccCode = consts.TccCode_Failed
			return
		}
	}
	// generate wal
	var cancelWal []byte
	{
		cancelWal = f.BusinessProvider.GenerateWalCancel(tccContext, lockKey, value, reservationWal)
	}
	// write tcc and mustWal in one transaction
	{
		b := &leveldb.Batch{}
		b.Put([]byte(vCancel.Key), []byte{})

		waliKey := fmt.Sprintf("wali_%s_%d", lockKey, value.GetVersion()+1)
		b.Put([]byte(waliKey), cancelWal)

		// write wal first
		err = f.DbRw.Write(b, f.WriteOption)
		if err != nil {
			log.Error().Err(err).Str("tcc", tccContext.String()).Msg("failed to write wal")
			return
		}
	}
	return
}

func (f *WalockStoreLevelDb) Update(lockKey model.LockerKey, updatedValue model.LockerValue,
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

func (f *WalockStoreLevelDb) FlushDirty() (err error) {
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
