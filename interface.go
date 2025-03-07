package walock

import (
	"context"
	"github.com/latifrons/walock/model"
	"gorm.io/gorm"
)

type CacheProvider interface {
	LoadAndLock(ctx context.Context, tx *gorm.DB, key model.LockerKey) (v model.LockerValue, err error)
	Unlock(key model.LockerKey)
	DoTry(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, tryBody interface{}) (tccCode model.TccCode, code string, message string, err error)
	DoConfirm(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, confirmBody interface{}) (tccCode model.TccCode, code string, message string, err error)
	DoCancel(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, cancelBody interface{}) (tccCode model.TccCode, code string, message string, err error)
	DoMust(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, mustBody interface{}) (tccCode model.TccCode, code string, essage string, err error)
	FlushDirty(tx *gorm.DB) (err error)
	Keys() []model.LockerKey
}

type WalProvider interface {
	CatchupWals(tx *gorm.DB, key model.LockerKey, load model.LockerValue) (err error)
	ApplyWal(load model.LockerValue, walis []interface{}) (err error)
	FlushWal(tx *gorm.DB, wali interface{}) (err error)
}

type PersistProvider interface {
	Load(tx *gorm.DB, key model.LockerKey) (value model.LockerValue, err error)
	Flush(tx *gorm.DB, value model.LockerValue) (err error)
}
type TccProvider interface {
	BarrierTry(tccContext *model.TccContext, tx *gorm.DB) (callIt bool, err error)
	BarrierConfirm(tccContext *model.TccContext, tx *gorm.DB) (callIt bool, err error)
	BarrierCancel(tccContext *model.TccContext, tx *gorm.DB) (callIt bool, err error)
	BarrierMust(tccContext *model.TccContext, tx *gorm.DB) (callIt bool, err error)
}

type TccBusinessProvider interface {
	TryWal(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, tryBody interface{}) (ok bool, code string, message string, tryWali interface{}, err error)
	ConfirmWal(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, reservationWali interface{}) (confirmWali interface{})
	CancelWal(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, reservationWali interface{}) (revertWali interface{})
	MustWal(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, mustBody interface{}) (ok bool, code string, message string, mustWali interface{}, err error)
	LoadReservation(tx *gorm.DB, tccContext *model.TccContext) (wal interface{}, ok bool, code string, message string, err error)
}
