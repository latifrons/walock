package walock

import (
	"context"
	"fmt"
	"github.com/latifrons/walock/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
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

type WalockStore struct {
	DbRw          *gorm.DB
	CacheProvider CacheProvider
	TccProvider   TccProvider
	MetricsName   string
	LockHoldTime  *prometheus.HistogramVec
}

func (w *WalockStore) Keys() (keys []model.LockerKey) {
	return w.CacheProvider.Keys()
}

func (w *WalockStore) Get(ctx context.Context, key model.LockerKey) (value model.LockerValue, err error) {
	valuePointer, err := w.CacheProvider.LoadAndLock(ctx, w.DbRw, key)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		w.LockHoldTime.WithLabelValues(w.MetricsName + "_get").Observe(time.Now().Sub(startTime).Seconds())
		w.CacheProvider.Unlock(key)
	}()

	value = valuePointer
	return
}

func (w *WalockStore) Must(ctx context.Context, tccContext *model.TccContext, lockKey model.LockerKey, mustBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	value, err := w.CacheProvider.LoadAndLock(ctx, w.DbRw, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		w.LockHoldTime.WithLabelValues(w.MetricsName + "_must").Observe(time.Now().Sub(startTime).Seconds())
		w.CacheProvider.Unlock(lockKey)
	}()

	exemptError := false // just to revert the transaction. do not return this error to caller

	err = w.DbRw.Transaction(func(tx *gorm.DB) error {
		var callIt bool
		callIt, err = w.TccProvider.BarrierMust(tccContext, tx)
		if err != nil {
			return err
		}
		if !callIt {
			tccCode = model.TccCode_Success
			message = "duplicate call"
			return nil
		}

		tccCode, code, message, err = w.CacheProvider.DoMust(tx, tccContext, lockKey, value, mustBody)
		if err != nil {
			return err
		}
		if tccCode != model.TccCode_Success {
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

func (w *WalockStore) Try(ctx context.Context, tccContext *model.TccContext, lockKey model.LockerKey, tryBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	value, err := w.CacheProvider.LoadAndLock(ctx, w.DbRw, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		w.LockHoldTime.WithLabelValues(w.MetricsName + "_try").Observe(time.Now().Sub(startTime).Seconds())
		w.CacheProvider.Unlock(lockKey)
	}()

	exemptError := false // just to revert the transaction. do not return this error to caller

	err = w.DbRw.Transaction(func(tx *gorm.DB) error {
		var callIt bool
		callIt, err = w.TccProvider.BarrierTry(tccContext, tx)
		if err != nil {
			return err
		}
		if !callIt {
			tccCode = model.TccCode_Success
			message = "duplicate call"
			return nil
		}

		tccCode, code, message, err = w.CacheProvider.DoTry(tx, tccContext, lockKey, value, tryBody)
		if err != nil {
			return err
		}
		if tccCode != model.TccCode_Success {
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

func (w *WalockStore) Confirm(ctx context.Context, tccContext *model.TccContext, lockKey model.LockerKey, confirmBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	exemptError := false // just to revert the transaction. do not return this error to caller

	err = w.DbRw.Transaction(func(tx *gorm.DB) error {
		var callIt bool
		callIt, err = w.TccProvider.BarrierConfirm(tccContext, tx)
		if err != nil {
			return err
		}
		if !callIt {
			tccCode = model.TccCode_Success
			message = "duplicate call"
			return nil
		}

		tccCode, code, message, err = w.CacheProvider.DoConfirm(tx, tccContext, lockKey, nil, confirmBody)
		if err != nil {
			return err
		}
		if tccCode != model.TccCode_Success {
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

func (w *WalockStore) Cancel(ctx context.Context, tccContext *model.TccContext, lockKey model.LockerKey, cancelBody interface{}) (tccCode model.TccCode, code string, message string, err error) {
	value, err := w.CacheProvider.LoadAndLock(ctx, w.DbRw, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		w.LockHoldTime.WithLabelValues(w.MetricsName + "_cancel").Observe(time.Now().Sub(startTime).Seconds())
		w.CacheProvider.Unlock(lockKey)
	}()

	exemptError := false // just to revert the transaction. do not return this error to caller

	err = w.DbRw.Transaction(func(tx *gorm.DB) error {
		var callIt bool
		callIt, err = w.TccProvider.BarrierCancel(tccContext, tx)
		if err != nil {
			return err
		}
		if !callIt {
			tccCode = model.TccCode_Success
			message = "duplicate call"
			return nil
		}
		tccCode, code, message, err = w.CacheProvider.DoCancel(tx, tccContext, lockKey, value, cancelBody)

		if err != nil {
			return err
		}
		if tccCode != model.TccCode_Success {
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

func (w *WalockStore) Update(ctx context.Context, lockKey model.LockerKey, updatedValue model.LockerValue,
	updater func(baseV, updateV model.LockerValue) (updated bool)) (err error) {
	baseValue, err := w.CacheProvider.LoadAndLock(ctx, w.DbRw, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		w.LockHoldTime.WithLabelValues(w.MetricsName + "_update").Observe(time.Now().Sub(startTime).Seconds())
		w.CacheProvider.Unlock(lockKey)
	}()

	updated := updater(baseValue, updatedValue)
	_ = updated
	return
}

func (w *WalockStore) FlushDirty() (err error) {
	return w.CacheProvider.FlushDirty(w.DbRw)
}

func (w *WalockStore) Traverse(f func(key model.LockerKey, value model.LockerValue) bool) {
	w.CacheProvider.Traverse(f)
}
