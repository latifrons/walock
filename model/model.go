package model

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

type Metrics struct {
	MetricsName         string
	MetricsLockWaitTime prometheus.Histogram
	MetricsMapCount     prometheus.Gauge
	LockHoldTime        *prometheus.HistogramVec
}

type Locker struct {
	Value LockerValue
	Mu    sync.Mutex
}

type LockerValue interface {
	GetVersion() uint64
	GetDbVersion() uint64
	SetDbVersion(v uint64)
	SetDirty(dirty bool)
	IsDirty() bool
}
type LockerKey string

type TccContext struct {
	GlobalId string
	BranchId string
}

func (s *TccContext) String() string {
	return s.GlobalId + "-" + s.BranchId
}

type TccCode int32

type TccBarrierReceiver struct {
	Key  string    `gorm:"size:100;primarykey"`
	Time time.Time `gorm:"index"`
}

type WalBytes []byte

type Wal struct {
	Key      string
	WalBytes WalBytes
}

func (w *Wal) String() string {
	return fmt.Sprintf("WAL K: %s, V: %s", w.Key, string(w.WalBytes))
}
