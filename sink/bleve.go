package sink

import (
	"github.com/LeeZXin/zsf-utils/quit"
	"github.com/LeeZXin/zsf-utils/randutil"
	"github.com/LeeZXin/zsf-utils/taskutil"
	"github.com/LeeZXin/zsf/logger"
	"github.com/LeeZXin/zsf/property/static"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/bwmarrin/snowflake"
	"github.com/robfig/cron/v3"
	"time"
	"zstash/model"
)

const (
	LogDefaultPath = "stash/bleve"
)

type BleveSink struct {
	logIndex bleve.Index
	idn      *snowflake.Node
}

func (s *BleveSink) Sink(c []taskutil.Chunk[model.LogContent]) {
	batch := s.logIndex.NewBatch()
	for _, t := range c {
		batch.Index(s.idn.Generate().String(), t.Data)
	}
	s.logIndex.Batch(batch)
}

func NewBleveSink() ISink {
	idn, err := snowflake.NewNode(randutil.Int63n(1024))
	if err != nil {
		logger.Logger.Panic(err)
	}
	m := bleve.NewIndexMapping()
	m.DefaultMapping = newBleveMapping()
	logIndex, err := bleve.NewUsing(LogDefaultPath, m, "upside_down", "goleveldb", map[string]any{
		"create_if_missing": true,
		"error_if_exists":   true,
	})
	if err != nil {
		logIndex, err = bleve.Open(LogDefaultPath)
		if err != nil {
			logger.Logger.Panic(err)
		}
	}
	quit.AddShutdownHook(func() {
		logIndex.Close()
	})
	ret := &BleveSink{
		logIndex: logIndex,
		idn:      idn,
	}
	ret.initCleanLogTask()
	return ret
}

func (s *BleveSink) initCleanLogTask() {
	// 开启定时任务清除n天前的日志
	if static.GetBool("bleve.clean.enabled") {
		cronExp := static.GetString("bleve.clean.cron")
		// 默认凌晨清除二十天之前的数据
		if cronExp == "" {
			cronExp = "0 0 0 * * ?"
		}
		cronTab := cron.New(cron.WithSeconds())
		_, err := cronTab.AddFunc(cronExp, func() {
			days := static.GetInt("bleve.clean.days")
			if days <= 0 {
				days = 20
			}
			endTime := time.Now().Add(time.Duration(days) * 24 * time.Hour).UnixMilli()
			s.cleanLog(endTime)
		})
		if err != nil {
			logger.Logger.Panic(err)
		}
		cronTab.Start()
		quit.AddShutdownHook(func() {
			cronTab.Stop()
		})
	}
}

func (s *BleveSink) cleanLog(end int64) {
	var endTime *float64
	if end > 0 {
		i := float64(end)
		endTime = &i
	}
	if endTime == nil {
		return
	}
	numeric := bleve.NewNumericRangeQuery(nil, endTime)
	numeric.SetField("timestamp")
	searchRequest := bleve.NewSearchRequest(numeric)
	searchRequest.Size = 1
	for {
		searchResult, err := s.logIndex.Search(searchRequest)
		if err != nil {
			return
		}
		if len(searchResult.Hits) == 0 {
			return
		}
		batch := s.logIndex.NewBatch()
		for _, hit := range searchResult.Hits {
			batch.Delete(hit.ID)
		}
		err = s.logIndex.Batch(batch)
		if err != nil {
			return
		}
	}
}

func newBleveMapping() *mapping.DocumentMapping {
	document := bleve.NewDocumentMapping()
	document.AddFieldMappingsAt("timestamp", bleve.NewNumericFieldMapping())
	document.AddFieldMappingsAt("level", bleve.NewTextFieldMapping())
	document.AddFieldMappingsAt("content", bleve.NewTextFieldMapping())
	document.AddFieldMappingsAt("application", bleve.NewTextFieldMapping())
	return document
}
