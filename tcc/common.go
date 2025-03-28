package tcc

import (
	"fmt"
	"github.com/latifrons/walock/model"
	"time"
)

func BuildTccBarrierReceiver(transactionType, globalId, branchId, branchType string) model.TccBarrierReceiver {
	return model.TccBarrierReceiver{
		Key:  fmt.Sprintf("%s-%s-%s-%s", transactionType, globalId, branchId, branchType),
		Time: time.Now(),
	}
}
