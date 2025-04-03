package walock

import (
	"errors"
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
	Metrics           *model.Metrics          // injected by outside to provide metrics inside
	BusinessProvider  BusinessProviderLevelDb // injected by outside to provide business logic
	TccBarrierLevelDb *tcc.TccBarrierLevelDb  // injected by outside to provide tcc barrier
	BarrierName       string
	WriteOption       *opt.WriteOptions

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

func (f *WalockStoreLevelDb) ensure(tx model.LevelDbStoreOperator, key model.LockerKey) (value model.LockerValue, err error) {
	value, err = f.BusinessProvider.LoadPersistedValue(key)
	if err != nil {
		log.Error().Err(err).Msg("failed to load from persist")
		return
	}
	var updated bool
	// replay wals
	updated, err = f.BusinessProvider.CatchupWals(tx, key, value)
	if err != nil {
		log.Error().Err(err).Msg("failed to catchup wals")
		return
	}

	if updated {
		err = f.BusinessProvider.PersistValue(value)
		if err != nil {
			return
		}
	}
	value.SetDirty(false)
	err = tx.MarkDirty([]byte(key), false, f.WriteOption)
	if err != nil {
		log.Error().Err(err).Str("key", string(key)).Msg("failed to clear dirty")
		return
	}

	return

}

func (f *WalockStoreLevelDb) LoadAndLock(tx model.LevelDbStoreOperator, key model.LockerKey) (lockValue model.LockerValue, err error) {
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
			log.Error().Any("v", r).Msg("recovered from panic")
			err = r.(error)
			lock.Mu.Unlock()
		} else if err != nil {
			lock.Mu.Unlock()
		}
	}()

	if lock.Value == nil {
		// load from database
		var newValue model.LockerValue
		newValue, err = f.ensure(tx, key)
		if err != nil {
			return
		}
		lock.Value = newValue
		log.Debug().Str("key", string(key)).Msg("loaded value from persist store")
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

func (f *WalockStoreLevelDb) Get(tx model.LevelDbStoreOperator, key model.LockerKey) (value model.LockerValue, err error) {

	valuePointer, err := f.LoadAndLock(tx, key)
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

func (f *WalockStoreLevelDb) Must(tx model.LevelDbStoreOperator, tccContext *model.TccContext, lockKey model.LockerKey, mustBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	value, err := f.LoadAndLock(tx, lockKey)
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
		callIt, err = f.TccBarrierLevelDb.CheckBarrierMust(tx, []byte(v.Key))
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
	var mustWal model.Wal
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
		b.Put([]byte(v.Key), []byte{})               // tcc barrier -> WAL key
		b.Put([]byte(mustWal.Key), mustWal.WalBytes) // WAL key
		//fmt.Println("PUT Must", mustWal.String())

		if !value.IsDirty() {
			err = tx.MarkDirty([]byte(lockKey), true, f.WriteOption)
			if err != nil {
				log.Error().Err(err).Str("tcc", tccContext.String()).Msg("failed to write dirty")
				return
			}
		}

		// write wal first
		err = tx.Write(b, f.WriteOption)
		if err != nil {
			log.Error().Err(err).Str("tcc", tccContext.String()).Msg("failed to write wal")
			return
		}
	}

	// update memory. this must success, or we will have a dirty wal
	value.SetDirty(true)
	f.BusinessProvider.MustApplyWal(value, []model.Wal{mustWal})
	tccCode = consts.TccCode_Success
	return

}

func (f *WalockStoreLevelDb) Try(tx model.LevelDbStoreOperator, tccContext *model.TccContext, lockKey model.LockerKey, tryBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	value, err := f.LoadAndLock(tx, lockKey)
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
		callIt, err = f.TccBarrierLevelDb.CheckBarrierTry(tx, []byte(v.Key))
		if err != nil {
			return
		}
		if !callIt {
			tccCode = consts.TccCode_Success
			message = "duplicate call"
			return
		}
	}
	//+-------------+-----------------------+-----------------------+
	//|             | Try                   | Cancel                |
	//+-------------+-----------------------+-----------------------+
	//| lockKey     | DUM-001#BTC           | DUM-001#BTC           |
	//+-------------+-----------------------+-----------------------+
	//| tcc barrier | B-GlobalId-BranchId-T | B-GlobalId-BranchId-X |
	//+-------------+-----------------------+-----------------------+
	//| wal Key     | DUM-001#BTC-100       | DUM-001#BTC-101       |
	//+-------------+-----------------------+-----------------------+

	// Caller Use Tcc Barrier to revert
	// I use Tcc Barrier to find Wal Key and then do the reverse Wal
	// So there must be a mapping between Tcc Barrier and WAL Key

	// generate wal
	var tryWal model.Wal
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
		b.Put([]byte(v.Key), []byte(tryWal.Key))   // tcc barrier -> WAL key
		b.Put([]byte(tryWal.Key), tryWal.WalBytes) // WAL key
		//fmt.Println("PUT Try", tryWal.String())

		if !value.IsDirty() {
			err = tx.MarkDirty([]byte(lockKey), true, f.WriteOption)
			if err != nil {
				log.Error().Err(err).Str("tcc", tccContext.String()).Msg("failed to write dirty")
				return
			}
		}

		// write wal first
		err = tx.Write(b, f.WriteOption)
		if err != nil {
			log.Error().Err(err).Str("tcc", tccContext.String()).Msg("failed to write wal")
			return
		}
	}

	// update memory. this must success, or we will have a dirty wal
	value.SetDirty(true)
	f.BusinessProvider.MustApplyWal(value, []model.Wal{tryWal})
	tccCode = consts.TccCode_Success

	return
}

func (f *WalockStoreLevelDb) Confirm(tx model.LevelDbStoreOperator, tccContext *model.TccContext, lockKey model.LockerKey, confirmBody interface{}) (tccCode model.TccCode, code string, message string, err error) {

	value, err := f.LoadAndLock(tx, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		f.Metrics.LockHoldTime.WithLabelValues(f.Metrics.MetricsName + "_must").Observe(time.Now().Sub(startTime).Seconds())
		f.Unlock(lockKey)
	}()

	vTry := tcc.BuildTccBarrierReceiver(f.BarrierName, tccContext.GlobalId, tccContext.BranchId, consts.TccBranchTypeTry)
	v := tcc.BuildTccBarrierReceiver(f.BarrierName, tccContext.GlobalId, tccContext.BranchId, consts.TccBranchTypeConfirm)

	// check TCC
	{
		var callIt bool
		callIt, err = f.TccBarrierLevelDb.CheckBarrierConfirm(tx, []byte(v.Key))
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
	var reservationWal model.Wal
	{
		var ok bool
		reservationWal, ok, code, message, err = f.LoadReservation(tx, vTry.Key)
		if err != nil {
			return
		}
		if !ok {
			tccCode = consts.TccCode_Failed
			return
		}
	}

	// generate wal
	var confirmWal model.Wal
	{
		confirmWal = f.BusinessProvider.GenerateWalConfirm(tccContext, lockKey, value, reservationWal)
	}
	// write tcc and mustWal in one transaction
	if confirmWal.Key != "" {
		b := &leveldb.Batch{}
		b.Put([]byte(v.Key), []byte{})
		b.Put([]byte(confirmWal.Key), confirmWal.WalBytes)
		//fmt.Println("PUT Confirm", confirmWal.String())

		if !value.IsDirty() {
			err = tx.MarkDirty([]byte(lockKey), true, f.WriteOption)
			if err != nil {
				log.Error().Err(err).Str("tcc", tccContext.String()).Msg("failed to write dirty")
				return
			}
		}

		// write wal first
		err = tx.Write(b, f.WriteOption)
		if err != nil {
			log.Error().Err(err).Str("tcc", tccContext.String()).Msg("failed to write wal")
			return
		}
		// update memory. this must success, or we will have a dirty wal
		value.SetDirty(true)
		f.BusinessProvider.MustApplyWal(value, []model.Wal{confirmWal})
	}

	tccCode = consts.TccCode_Success
	return
}

func (f *WalockStoreLevelDb) Cancel(tx model.LevelDbStoreOperator, tccContext *model.TccContext, lockKey model.LockerKey, cancelBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	value, err := f.LoadAndLock(tx, lockKey)
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
		callIt, err = f.TccBarrierLevelDb.CheckBarrierCancel(tx, []byte(vTry.Key), []byte(vCancel.Key))
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
	var reservationWal model.Wal
	{
		var ok bool
		reservationWal, ok, code, message, err = f.LoadReservation(tx, vTry.Key)
		if err != nil {
			return
		}
		if !ok {
			tccCode = consts.TccCode_Failed
			return
		}
	}
	// generate wal
	var cancelWal model.Wal
	{
		cancelWal = f.BusinessProvider.GenerateWalCancel(tccContext, lockKey, value, reservationWal)
	}
	// write tcc and mustWal in one transaction
	{
		b := &leveldb.Batch{}
		b.Put([]byte(vCancel.Key), []byte{})             // tcc barrier -> WAL key
		b.Put([]byte(cancelWal.Key), cancelWal.WalBytes) // WAL key
		//fmt.Println("PUT Cancel", cancelWal.String())

		if !value.IsDirty() {
			err = tx.MarkDirty([]byte(lockKey), true, f.WriteOption)
			if err != nil {
				log.Error().Err(err).Str("tcc", tccContext.String()).Msg("failed to write dirty")
				return
			}
		}

		// write wal first
		err = tx.Write(b, f.WriteOption)
		if err != nil {
			log.Error().Err(err).Str("tcc", tccContext.String()).Msg("failed to write wal")
			return
		}
	}
	value.SetDirty(true)
	f.BusinessProvider.MustApplyWal(value, []model.Wal{cancelWal})
	tccCode = consts.TccCode_Success

	return
}

func (f *WalockStoreLevelDb) Update(tx model.LevelDbStoreOperator, lockKey model.LockerKey, updatedValue model.LockerValue,
	updater func(baseV, updateV model.LockerValue) (updated bool)) (err error) {
	baseValue, err := f.LoadAndLock(tx, lockKey)
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

func (f *WalockStoreLevelDb) FlushDirty(tx model.LevelDbStoreOperator) (err error) {
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
			err = f.BusinessProvider.PersistValue(lock.Value)
			if err != nil {
				log.Error().Err(err).Msg("failed to flush back")
				return false
			}
			lock.Value.SetDbVersion(lock.Value.GetVersion())

			lock.Value.SetDirty(false)
			err = tx.MarkDirty([]byte(key.(string)), false, f.WriteOption)
			if err != nil {
				log.Error().Err(err).Str("key", key.(string)).Msg("failed to clear dirty")
			}

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

func (f *WalockStoreLevelDb) LoadReservation(tx model.LevelDbStoreOperator, tryBarrierKey string) (wal model.Wal, ok bool, code string, message string, err error) {
	walId, err := tx.Get([]byte(tryBarrierKey), nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			ok = false
			code = consts.ErrReservationNotFound
			message = "reservation not found from barrier: " + tryBarrierKey
			return
		}
		log.Error().Err(err).Msg("failed to load reservation")
		return
	}

	walBytes, err := tx.Get(walId, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			ok = false
			code = consts.ErrReservationNotFound
			message = "reservation not found from wal: " + string(walId)
			return
		}
		log.Error().Err(err).Msg("failed to load reservation")
		return
	}
	wal = model.Wal{
		Key:      string(walId),
		WalBytes: walBytes,
	}

	ok = true
	return
}

func (f *WalockStoreLevelDb) ClearDirtyRecords(tx model.LevelDbStoreOperator) (err error) {
	// clear dirty by access the record once so that wal will be replayed
	var dirtyKeys [][]byte
	dirtyKeys, err = tx.ListDirty()
	if err != nil {
		log.Error().Err(err).Msg("failed to list dirty keys")
		return
	}

	for _, key := range dirtyKeys {
		log.Info().Str("key", string(key)).Msg("clearing dirty key")
		var v model.LockerValue
		v, err = f.Get(tx, model.LockerKey(key))
		if err != nil {
			log.Error().Err(err).Str("key", string(key)).Msg("failed to clear dirty key")
			return err
		} else {
			log.Info().Str("key", string(key)).Uint64("version", v.GetVersion()).Uint64("dbVersion", v.GetDbVersion()).Msg("cleared dirty key")
		}
	}
	return
}
