package consumer

import (
	"context"
	"encoding/json"
	"github.com/LeeZXin/zsf/logger"
	"github.com/LeeZXin/zsf/mq"
	"github.com/LeeZXin/zsf/property/static"
	"github.com/nsqio/go-nsq"
	"strings"
	"zstash/model"
	"zstash/sink"
)

func InitNsq() {
	executorNum := static.GetInt("nsq.consumer.executorNums")
	if executorNum <= 0 {
		logger.Logger.Panic("nsq executorNums should greater than 0")
	}
	consumer, err := mq.NewNsqConsumer(mq.NsqConsumerConfig{
		Topic:        static.GetString("nsq.consumer.topic"),
		Channel:      static.GetString("nsq.consumer.channel"),
		Addrs:        strings.Split(static.GetString("nsq.consumer.addrs"), ";"),
		ExecutorNums: static.GetInt("nsq.consumer.executorNums"),
		AuthSecret:   static.GetString("nsq.consumer.auth"),
	})
	if err != nil {
		logger.Logger.Panic(err)
	}
	consumer.ConsumeNsqds(func(ctx context.Context, message *nsq.Message) error {
		var log model.LogContent
		// 不符合格式
		err := json.Unmarshal(message.Body, &log)
		if err != nil {
			return nil
		}
		sink.DoSink(log)
		return nil
	})
}
