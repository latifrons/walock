package consts

import "github.com/latifrons/walock/model"

const (
	TccCode_Success model.TccCode = 0
	TccCode_Failed  model.TccCode = 1
	TccCode_Timeout model.TccCode = 2
)
const (
	TccBranchTypeTry     = "T"
	TccBranchTypeConfirm = "C"
	TccBranchTypeCancel  = "X"
	TccBranchTypeMust    = "M"
)

const ErrReservationNotFound = "ErrReservationNotFound"
