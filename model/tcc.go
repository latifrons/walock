package model

type TccContext struct {
	GlobalId string
	BranchId string
}

func (s *TccContext) String() string {
	return s.GlobalId + "-" + s.BranchId
}

type TccCode int32

const (
	TccCode_Success TccCode = 0
	TccCode_Failed  TccCode = 1
	TccCode_Timeout TccCode = 2
)
