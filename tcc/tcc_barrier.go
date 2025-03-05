package tcc

import (
	"fmt"
	"github.com/latifrons/walock/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

const (
	TccBranchTypeTry     = "T"
	TccBranchTypeConfirm = "C"
	TccBranchTypeCancel  = "X"
	TccBranchTypeMust    = "M"
)

type TccBarrier struct {
	BarrierName string
}

func buildTccBarrierReceiver(transactionType, globalId, branchId, branchType string) model.TccBarrierReceiver {
	return model.TccBarrierReceiver{
		Key:  fmt.Sprintf("%s-%s-%s-%s", transactionType, globalId, branchId, branchType),
		Time: time.Now(),
	}
}

func (f *TccBarrier) BarrierMust(tccHeader *model.TccContext, pbtx *gorm.DB) (callIt bool, err error) {
	// 如果是Try分支，则那么insert ignore插入gid-branchid-try，如果成功插入，则调用屏障内逻辑
	v := buildTccBarrierReceiver(f.BarrierName, tccHeader.GlobalId, tccHeader.BranchId, TccBranchTypeMust)
	result := pbtx.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(&v)

	if result.Error != nil {
		err = result.Error
		return
	}
	if result.RowsAffected == 1 {
		callIt = true
		return
	}
	return
}

func (f *TccBarrier) BarrierTry(tccHeader *model.TccContext, pbtx *gorm.DB) (callIt bool, err error) {
	// 如果是Try分支，则那么insert ignore插入gid-branchid-try，如果成功插入，则调用屏障内逻辑
	v := buildTccBarrierReceiver(f.BarrierName, tccHeader.GlobalId, tccHeader.BranchId, TccBranchTypeTry)
	result := pbtx.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(&v)

	if result.Error != nil {
		err = result.Error
		return
	}
	if result.RowsAffected == 1 {
		callIt = true
		return
	}
	return
}
func (f *TccBarrier) BarrierConfirm(tccHeader *model.TccContext, pbtx *gorm.DB) (callIt bool, err error) {
	// 如果是Confirm分支，那么insert ignore插入gid-branchid-confirm，如果成功插入，则调用屏障内逻辑
	v := buildTccBarrierReceiver(f.BarrierName, tccHeader.GlobalId, tccHeader.BranchId, TccBranchTypeConfirm)
	result := pbtx.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(&v)

	if result.Error != nil {
		err = result.Error
		return
	}
	if result.RowsAffected == 1 {
		callIt = true
	}
	return
}

func (f *TccBarrier) BarrierCancel(tccHeader *model.TccContext, pbtx *gorm.DB) (callIt bool, err error) {
	// 如果是Cancel分支，那么insert ignore插入gid-branchid-try，再插入gid-branchid-cancel，如果try未插入并且cancel插入成功，则调用屏障内逻辑
	v := buildTccBarrierReceiver(f.BarrierName, tccHeader.GlobalId, tccHeader.BranchId, TccBranchTypeTry)
	result := pbtx.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(&v)

	if result.Error != nil {
		err = result.Error
		return
	}
	if result.RowsAffected != 0 { // must be 0 to continue
		return
	}

	// check if the branch is cancelled
	v = buildTccBarrierReceiver(f.BarrierName, tccHeader.GlobalId, tccHeader.BranchId, TccBranchTypeCancel)
	result = pbtx.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(&v)

	if result.Error != nil {
		err = result.Error
		return
	}
	if result.RowsAffected == 1 {
		callIt = true
	}

	return
}
