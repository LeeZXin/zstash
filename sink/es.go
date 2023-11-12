package sink

import (
	"github.com/LeeZXin/zsf-utils/taskutil"
	"zstash/model"
)

type EsSink struct {
}

func (*EsSink) Sink(c []taskutil.Chunk[model.LogContent]) {

}

func NewEsSink() ISink {
	return &EsSink{}
}
