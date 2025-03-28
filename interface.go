package walock

import (
	"github.com/latifrons/walock/model"
	"github.com/syndtr/goleveldb/leveldb"
	"gorm.io/gorm"
)

//type CacheProvider interface {
//	LoadAndLock(ctx context.Context, tx *gorm.DB, key model.LockerKey) (v model.LockerValue, err error)
//	Unlock(key model.LockerKey)
//	DoTry(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, tryBody interface{}) (tccCode model.TccCode, code string, message string, err error)
//	DoConfirm(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, confirmBody interface{}) (tccCode model.TccCode, code string, message string, err error)
//	DoCancel(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, cancelBody interface{}) (tccCode model.TccCode, code string, message string, err error)
//	DoMust(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, mustBody interface{}) (tccCode model.TccCode, code string, essage string, err error)
//	FlushDirty(tx *gorm.DB) (err error)
//	Traverse(func(key model.LockerKey, value model.LockerValue) bool)
//	Keys() []model.LockerKey
//}

// BusinessProviderSql is the interface for business logic provider.
// dbContext is usually a *gorm.DB, but can be any type, like leveldb.DB, sql.DB, etc.
type BusinessProviderSql interface {
	LoadPersistedValue(tx *gorm.DB, key model.LockerKey) (v model.LockerValue, err error)
	GenerateWalTry(tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, tryBody interface{}) (ok bool, code string, message string, tryWali interface{}, err error)
	GenerateWalConfirm(tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, reservationWali interface{}) (confirmWali interface{})
	GenerateWalCancel(tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, reservationWali interface{}) (revertWali interface{})
	GenerateWalMust(tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, mustBody interface{}) (ok bool, code string, message string, mustWali interface{}, err error)
	LoadReservation(tx *gorm.DB, tccContext *model.TccContext) (wal interface{}, ok bool, code string, message string, err error)
	CatchupWals(tx *gorm.DB, key model.LockerKey, load model.LockerValue) (err error)
	ApplyWal(load model.LockerValue, walis []interface{}) (err error)
	FlushWal(tx *gorm.DB, wali interface{}) error
	FlushDirty(tx *gorm.DB) (err error)
	Traverse(func(key model.LockerKey, value model.LockerValue) bool)
	Keys() []model.LockerKey
	Flush(tx *gorm.DB, value model.LockerValue) error
}

type WalBytes []byte

type BusinessProviderLevelDb interface {
	LoadPersistedValue(tx *leveldb.DB, key model.LockerKey) (v model.LockerValue, err error)
	GenerateWalTry(tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, tryBody interface{}) (ok bool, code string, message string, tryWali WalBytes, err error)
	GenerateWalConfirm(tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, reservationWali WalBytes) (confirmWali WalBytes)
	GenerateWalCancel(tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, reservationWali WalBytes) (revertWali WalBytes)
	GenerateWalMust(tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, mustBody interface{}) (ok bool, code string, message string, mustWali WalBytes, err error)
	LoadReservation(tx *leveldb.DB, tccContext *model.TccContext) (wal WalBytes, ok bool, code string, message string, err error)
	CatchupWals(tx *leveldb.DB, key model.LockerKey, load model.LockerValue) (err error)
	MustApplyWal(load model.LockerValue, walis []WalBytes)
	FlushWal(tx *leveldb.DB, wali WalBytes) error
	FlushDirty(tx *gorm.DB) (err error)
	Traverse(func(key model.LockerKey, value model.LockerValue) bool)
	Keys() []model.LockerKey
	Flush(tx *leveldb.DB, value model.LockerValue) error
}

//
//type WalProvider interface {
//	CatchupWals(tx *gorm.DB, key model.LockerKey, load model.LockerValue) (err error)
//	ApplyWal(load model.LockerValue, walis []interface{}) (err error)
//	FlushWal(tx *gorm.DB, wali interface{}) (err error)
//}

//type PersistProvider interface {
//	Load(tx *gorm.DB, key model.LockerKey) (value model.LockerValue, err error)
//	Flush(tx *gorm.DB, value model.LockerValue) (err error)
//}

//type TccProvider interface {
//	BarrierTry(tccContext *model.TccContext, tx *gorm.DB) (callIt bool, err error)
//	BarrierConfirm(tccContext *model.TccContext, tx *gorm.DB) (callIt bool, err error)
//	BarrierCancel(tccContext *model.TccContext, tx *gorm.DB) (callIt bool, err error)
//	BarrierMust(tccContext *model.TccContext, tx *gorm.DB) (callIt bool, err error)
//}

//type TccBusinessProvider interface {
//	// ok: if the action is successful.
//	// code: the business error code. "" if ok is true.
//	// message: the error message. "" if ok is true.
//	// tryWali: the try WAL object to be persisted. nil if ok is false.
//	// err: any system error. nil if ok is true.
//	TryWal(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, tryBody interface{}) (ok bool, code string, message string, tryWali interface{}, err error)
//	ConfirmWal(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, reservationWali interface{}) (confirmWali interface{})
//	CancelWal(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, reservationWali interface{}) (revertWali interface{})
//	MustWal(tx *gorm.DB, tccContext *model.TccContext, key model.LockerKey, value model.LockerValue, mustBody interface{}) (ok bool, code string, message string, mustWali interface{}, err error)
//	LoadReservation(tx *gorm.DB, tccContext *model.TccContext) (wal interface{}, ok bool, code string, message string, err error)
//}
