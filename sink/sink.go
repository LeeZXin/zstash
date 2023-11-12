package sink

import (
	"github.com/LeeZXin/zsf-utils/quit"
	"github.com/LeeZXin/zsf-utils/taskutil"
	"github.com/LeeZXin/zsf/logger"
	"github.com/LeeZXin/zsf/property/static"
	"time"
	"zstash/model"
)

var (
	sinkType = static.GetString("sink.type")

	task *taskutil.ChunkTask[model.LogContent]

	sinkImpl ISink
)

func init() {
	switch sinkType {
	case "bleve":
		sinkImpl = NewBleveSink()
	case "es":
		sinkImpl = NewEsSink()
	default:
		logger.Logger.Panic("unsupported sink type")
	}
	triggerSize := static.GetInt("sink.task.triggerSize")
	if triggerSize <= 0 {
		triggerSize = 10240
	}
	flushDuration := static.GetInt("sink.task.flushDuration")
	if flushDuration <= 0 {
		flushDuration = 3
	}
	var err error
	task, err = taskutil.NewChunkTask[model.LogContent](
		triggerSize,
		sinkImpl.Sink,
		time.Duration(flushDuration)*time.Second,
	)
	if err != nil {
		panic(err)
	}
	task.Start()
	quit.AddShutdownHook(func() {
		task.Stop()
	})
}

type ISink interface {
	Sink([]taskutil.Chunk[model.LogContent])
}

func DoSink(content model.LogContent) {
	task.Execute(content, 1)
}
