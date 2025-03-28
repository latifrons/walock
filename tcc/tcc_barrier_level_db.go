package tcc

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
)

type TccBarrierLevelDb struct {
	BarrierName string
	DbTableName string
}

func (f *TccBarrierLevelDb) CheckBarrierMust(mustKey []byte, ldb *leveldb.DB) (callIt bool, err error) {
	// 如果是Try分支，则那么insert ignore插入gid-branchid-try，如果成功插入，则调用屏障内逻辑
	set, _, err := CheckNX(ldb, mustKey)
	if err != nil {
		return
	}
	callIt = set
	return
}

func (f *TccBarrierLevelDb) CheckBarrierTry(tryKey []byte, ldb *leveldb.DB) (callIt bool, err error) {
	// 如果是Try分支，则那么insert ignore插入gid-branchid-try，如果成功插入，则调用屏障内逻辑
	set, _, err := CheckNX(ldb, tryKey)
	if err != nil {
		return
	}
	callIt = set
	return
}
func (f *TccBarrierLevelDb) CheckBarrierConfirm(confirmKey []byte, ldb *leveldb.DB) (callIt bool, err error) {
	// 如果是Confirm分支，那么insert ignore插入gid-branchid-confirm，如果成功插入，则调用屏障内逻辑
	set, _, err := CheckNX(ldb, confirmKey)
	if err != nil {
		return
	}
	callIt = set
	return
}

func (f *TccBarrierLevelDb) CheckBarrierCancel(tryKey []byte, cancelKey []byte, ldb *leveldb.DB) (callIt bool, err error) {
	// 如果是Cancel分支，那么insert ignore插入gid-branchid-try，再插入gid-branchid-cancel，如果try未插入并且cancel插入成功，则调用屏障内逻辑
	set, _, err := CheckNX(ldb, tryKey)
	if err != nil {
		return
	}
	if set {
		// try分支插入成功，直接返回
		return
	}

	// check if the branch is cancelled
	set, _, err = CheckNX(ldb, cancelKey)
	if err != nil {
		return
	}
	if set {
		callIt = true
	}
	return
}

// CheckNX
// It returns true if the key does not exist and false if it does exist
func CheckNX(db *leveldb.DB, key []byte) (notExists bool, value []byte, err error) {
	// Try to get the existing value
	value, err = db.Get(key, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			// Key does not exist, set the new value
			notExists = true
			//err = db.Put(key, setValue, nil)
			//if err != nil {
			//	return
			//}
			//value = setValue
		} else {
			return
		}
	}
	return

}

// SetNX sets a key in the database if it does not exist
// It returns true if the key was set, or false if it already existed
func SetNX(db *leveldb.DB, key []byte, setValue []byte) (set bool, value []byte, err error) {
	// Try to get the existing value
	value, err = db.Get(key, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			// Key does not exist, set the new value
			set = true
			err = db.Put(key, setValue, nil)
			if err != nil {
				return
			}
			value = setValue
		} else {
			return
		}
	}
	return

}
