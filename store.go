package walock

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"walock/model"
)

import (
	"context"
	"gorm.io/gorm"
	"time"
)

type WalockStore struct {
	QuotaDbRw   *gorm.DB `container:"name"`
	MetricsName string

	CacheProvider   CacheProvider
	WalProvider     WalProvider
	PersistProvider PersistProvider
	TccProvider     TccProvider

	LockHoldTime *prometheus.HistogramVec
}

func (w *WalockStore) Get(ctx context.Context, key string) (value model.LockerValue, err error) {
	valuePointer, err := w.CacheProvider.LoadAndLock(ctx, key)
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

func (w *WalockStore) Try(ctx context.Context, tccContext *model.TccContext, lockKey string, tryBody interface{}) (code model.TccCode, message string, err error) {
	value, err := w.CacheProvider.LoadAndLock(ctx, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		w.LockHoldTime.WithLabelValues(w.MetricsName + "_try").Observe(time.Now().Sub(startTime).Seconds())
		w.CacheProvider.Unlock(lockKey)
	}()

	err = w.QuotaDbRw.Transaction(func(tx *gorm.DB) error {
		var callIt bool
		callIt, err = w.TccProvider.BarrierTry(tccContext, tx)
		if err != nil {
			return err
		}
		if !callIt {
			code = model.TccCode_Success
			message = "duplicate call"
			return nil
		}

		code, message, err = w.CacheProvider.DoTry(tx, tccContext, value, tryBody)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Debug().Str("gid", tccContext.GlobalId).Str("bid", tccContext.BranchId).Err(err).Msg("tx reverted Try")
		return
	}
	return
}

func (w *WalockStore) Confirm(ctx context.Context, tccContext *model.TccContext, lockKey string, confirmBody interface{}) (code model.TccCode, message string, err error) {
	err = w.QuotaDbRw.Transaction(func(tx *gorm.DB) error {
		var callIt bool
		callIt, err = w.TccProvider.BarrierConfirm(tccContext, tx)
		if err != nil {
			return err
		}
		if !callIt {
			code = model.TccCode_Success
			message = "duplicate call"
			return nil
		}

		code, message, err = w.CacheProvider.DoConfirm(tx, tccContext, lockKey, confirmBody)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Debug().Str("gid", tccContext.GlobalId).Str("bid", tccContext.BranchId).Err(err).Msg("tx reverted Confirm")
		return
	}
	return
}

func (w *WalockStore) Cancel(ctx context.Context, tccContext *model.TccContext, lockKey string, cancelBody interface{}) (code model.TccCode, message string, err error) {
	valuePointer, err := w.CacheProvider.LoadAndLock(ctx, lockKey)
	if err != nil {
		return
	}

	startTime := time.Now()
	defer func() {
		w.LockHoldTime.WithLabelValues(w.MetricsName + "_cancel").Observe(time.Now().Sub(startTime).Seconds())
		w.CacheProvider.Unlock(lockKey)
	}()

	err = w.QuotaDbRw.Transaction(func(tx *gorm.DB) error {
		var callIt bool
		callIt, err = w.TccProvider.BarrierCancel(tccContext, tx)
		if err != nil {
			return err
		}
		if !callIt {
			code = model.TccCode_Success
			message = "duplicate call"
			return nil
		}
		code, message, err = w.CacheProvider.DoCancel(tx, tccContext, valuePointer, cancelBody)

		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Debug().Str("gid", tccContext.GlobalId).Str("bid", tccContext.BranchId).Err(err).Msg("tx reverted Cancel")
		return
	}
	return
}
