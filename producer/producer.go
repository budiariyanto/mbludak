package producer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
)

func Dispatch(appContext AppContext) (DispatchStatistics, error) {
	cfg := sarama.NewConfig()
	cfg.Net.SASL.User = appContext.Username
	cfg.Net.SASL.Password = appContext.Password

	producer, err := sarama.NewAsyncProducer([]string{appContext.Brokers}, cfg)
	if err != nil {
		panic(err)
	}

	defer func(asyncProducer sarama.AsyncProducer) {
		if err := asyncProducer.Close(); err != nil {
			// TODO: log
			fmt.Println(err)
		}
	}(producer)

	fileContent, err := ioutil.ReadFile(appContext.File)
	if err != nil {
		return DispatchStatistics{}, err
	}

	if appContext.Format == "json" {
		if !json.Valid(fileContent) {
			return DispatchStatistics{}, errors.New("dispatch: Invalid json")
		}
	}

	var collection []map[string]interface{}
	err = json.Unmarshal(fileContent, &collection)
	if err != nil {
		return DispatchStatistics{}, err
	}

	totalData := int64(len(collection))
	channel := make(chan Result, totalData)
	var errCount int64

	for _, v := range collection {
		for _, field := range appContext.RandomFields {
			randomValue := field.Generate()
			if randomValue != nil {
				v[field.Name] = randomValue
			}
		}

		data, _ := json.Marshal(v)

		err := publish(producer, appContext.Topic, data, channel)
		if err != nil {
			errCount++
			continue
		}
	}

	result := <- channel
	if !result.Success {
		errCount++
		log.Printf("Error publishing data: err: %v, original_data: %+v\n", result.Error, result.OriginalMessage)
	}

	stats := DispatchStatistics{
		SuccessCount: totalData - errCount,
		FailedCount:  errCount,
		Total:        int64(totalData),
	}

	return stats, nil
}

type Result struct {
	Success bool
	OriginalMessage string
	Error error
}

type DispatchStatistics struct {
	SuccessCount int64
	FailedCount int64
	Total int64
}

func publish(producer sarama.AsyncProducer, topic string, data []byte, pChannel chan Result) error {
	if !json.Valid(data) {
		return errors.New("publish: Invalid json")
	}

	pmsg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   nil,
		Value: sarama.ByteEncoder(data),
	}

	select {
	case producer.Input() <- pmsg:
		pChannel <- Result{
			Success:         true,
			OriginalMessage: string(data),
		}
	case err := <-producer.Errors():
		pChannel <- Result{
			Success:         false,
			OriginalMessage: string(data),
			Error: err,
		}
	}

	return nil
}