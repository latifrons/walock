package tcc

import (
	"github.com/latifrons/walock/consts"
	"github.com/latifrons/walock/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type TccBarrierSql struct {
	BarrierName string
	DbTableName string
}

// BarrierMust is protected by a lockKey level mutex
func (f *TccBarrierSql) BarrierMust(tccHeader *model.TccContext, persistentContext interface{}) (callIt bool, err error) {
	pbtx := persistentContext.(*gorm.DB)

	// 如果是Try分支，则那么insert ignore插入gid-branchid-try，如果成功插入，则调用屏障内逻辑
	v := BuildTccBarrierReceiver(f.BarrierName, tccHeader.GlobalId, tccHeader.BranchId, consts.TccBranchTypeMust)
	result := pbtx.Clauses(clause.Insert{Modifier: "IGNORE"}).Table(f.DbTableName).Create(&v)

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

// BarrierTry is protected by a lockKey level mutex
func (f *TccBarrierSql) BarrierTry(tccHeader *model.TccContext, persistentContext interface{}) (callIt bool, err error) {
	pbtx := persistentContext.(*gorm.DB)

	// 如果是Try分支，则那么insert ignore插入gid-branchid-try，如果成功插入，则调用屏障内逻辑
	v := BuildTccBarrierReceiver(f.BarrierName, tccHeader.GlobalId, tccHeader.BranchId, consts.TccBranchTypeTry)
	result := pbtx.Clauses(clause.Insert{Modifier: "IGNORE"}).Table(f.DbTableName).Create(&v)

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

// BarrierConfirm is protected by a lockKey level mutex
func (f *TccBarrierSql) BarrierConfirm(tccHeader *model.TccContext, persistentContext interface{}) (callIt bool, err error) {
	pbtx := persistentContext.(*gorm.DB)

	// 如果是Confirm分支，那么insert ignore插入gid-branchid-confirm，如果成功插入，则调用屏障内逻辑
	v := BuildTccBarrierReceiver(f.BarrierName, tccHeader.GlobalId, tccHeader.BranchId, consts.TccBranchTypeConfirm)
	result := pbtx.Clauses(clause.Insert{Modifier: "IGNORE"}).Table(f.DbTableName).Create(&v)

	if result.Error != nil {
		err = result.Error
		return
	}
	if result.RowsAffected == 1 {
		callIt = true
	}
	return
}

// BarrierCancel is protected by a lockKey level mutex
func (f *TccBarrierSql) BarrierCancel(tccHeader *model.TccContext, persistentContext interface{}) (callIt bool, err error) {
	pbtx := persistentContext.(*gorm.DB)

	// 如果是Cancel分支，那么insert ignore插入gid-branchid-try，再插入gid-branchid-cancel，如果try未插入并且cancel插入成功，则调用屏障内逻辑
	v := BuildTccBarrierReceiver(f.BarrierName, tccHeader.GlobalId, tccHeader.BranchId, consts.TccBranchTypeTry)
	result := pbtx.Clauses(clause.Insert{Modifier: "IGNORE"}).Table(f.DbTableName).Create(&v)

	if result.Error != nil {
		err = result.Error
		return
	}
	if result.RowsAffected != 0 { // must be 0 to continue
		return
	}

	// check if the branch is cancelled
	v = BuildTccBarrierReceiver(f.BarrierName, tccHeader.GlobalId, tccHeader.BranchId, consts.TccBranchTypeCancel)
	result = pbtx.Clauses(clause.Insert{Modifier: "IGNORE"}).Table(f.DbTableName).Create(&v)

	if result.Error != nil {
		err = result.Error
		return
	}
	if result.RowsAffected == 1 {
		callIt = true
	}

	return
}
