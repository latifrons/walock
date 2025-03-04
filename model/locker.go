package model

import "sync"

type Locker struct {
	Value LockerValue
	Mu    sync.Mutex
}

type LockerValue interface {
	GetVersion() uint64
	GetDbVersion() uint64
}

type LockerKey string
