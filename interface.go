package walock

import (
	"context"
	"gorm.io/gorm"
	"walock/model"
)

type CacheProvider interface {
	LoadAndLock(ctx context.Context, key string) (v model.LockerValue, err error)
	Unlock(key string)
	DoTry(tx *gorm.DB, tccContext *model.TccContext, value model.LockerValue, tryBody interface{}) (code model.TccCode, message string, err error)
	DoConfirm(tx *gorm.DB, tccContext *model.TccContext, key string, confirmBody interface{}) (model.TccCode, string, error)
	DoCancel(tx *gorm.DB, tccContext *model.TccContext, key model.LockerValue, cancelBody interface{}) (model.TccCode, string, error)
}

type WalProvider interface {
}

type PersistProvider interface {
	Load(tx *gorm.DB, key string) (value model.LockerValue, err error)
}

type TccProvider interface {
	BarrierTry(tccContext *model.TccContext, tx *gorm.DB) (callIt bool, err error)
	BarrierConfirm(tccContext *model.TccContext, tx *gorm.DB) (callIt bool, err error)
	BarrierCancel(tccContext *model.TccContext, tx *gorm.DB) (callIt bool, err error)
}

type TccBusinessProvider interface {
	Try(tx *gorm.DB, tccContext *model.TccContext, value model.LockerValue, body interface{}) (ok bool, code string, message string, err error)
	Confirm(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, body interface{}) (ok bool, code string, message string, err error)
	Cancel(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, body interface{}) (ok bool, code string, message string, err error)
}
