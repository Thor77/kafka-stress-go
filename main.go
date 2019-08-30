package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var checkpointInterval = 1000

type TimestampContainer struct {
	UnixTimestamp int64  `json:"timestamp"`
	Data          int    `json:"data"`
	DataString    string `json:"data_string"`
}

type Checkpoint struct {
	Thread  string
	Elapsed time.Duration
}

func randomString() string {
	return randomStringN(1000)
}

func randomStringN(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func readCheckpoints(checkpointChannel chan Checkpoint) {
	var checkpoint Checkpoint
	for {
		checkpoint = <-checkpointChannel
		log.Printf("[%s] %d messages sent in %s (rate: %f msg/s)", checkpoint.Thread, checkpointInterval, checkpoint.Elapsed, float64(checkpointInterval)/checkpoint.Elapsed.Seconds())
	}
}

func produceMessages(produceChannel chan *kafka.Message, topicPartition kafka.TopicPartition, start int, end int, delayDuration time.Duration, checkpointChannel chan Checkpoint, thread string, dataFunc func() string) {
	i := 0
	startTime := time.Now()
	for start != end {
		tc := TimestampContainer{UnixTimestamp: time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond)), Data: 2, DataString: dataFunc()}
		value, err := json.Marshal(tc)
		if err != nil {
			log.Print(err)
		}
		produceChannel <- &kafka.Message{TopicPartition: topicPartition, Value: value}
		if end != -1 {
			start = start + 1
		} else {
			i = i - 1
			if i <= 0 {
				if i == 0 {
					checkpointChannel <- Checkpoint{Thread: thread, Elapsed: time.Now().Sub(startTime)}
				}
				i = checkpointInterval
				startTime = time.Now()
			}
		}
		// sleep...
		time.Sleep(delayDuration)
	}
}

type Config struct {
	Topic              string          `json:"topic"`
	Topics             []string        `json:"topics"`
	CheckpointInterval int             `json:"checkpoint_interval"`
	ProducerConfig     kafka.ConfigMap `json:"producer"`
	WordCount          int             `json:"word_count"`
	WordLength         int             `json:"word_length"`
}

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("%s <configfile> <num messages> <delay> [<num threads>]", os.Args[0])
	}

	// read producer config
	configRaw, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	var config Config
	if err := json.Unmarshal(configRaw, &config); err != nil {
		log.Fatal(err)
	}

	if config.CheckpointInterval != 0 {
		checkpointInterval = config.CheckpointInterval
	}

	producer, err := kafka.NewProducer(&config.ProducerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	// start event listener
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				}
			}
		}
	}()

	topicPartition := kafka.TopicPartition{Topic: &config.Topic, Partition: kafka.PartitionAny}

	produceChannel := producer.ProduceChannel()

	numMessages, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	delayDuration, err := time.ParseDuration(os.Args[3])
	if err != nil {
		log.Fatal(err)
	}

	if config.WordLength == 0 {
		// set default value
		config.WordLength = 1000
	}

	if config.WordCount == 0 {
		// set default value
		config.WordCount = 1
	}

	// build word list
	var words []string
	for config.WordCount > 0 {
		words = append(words, randomStringN(config.WordLength))
		config.WordCount = config.WordCount - 1
	}

	var dataFunc func() string
	if len(words) > 1 {
		dataFunc = func() string {
			r := make([]string, len(words))
			perm := rand.Perm(len(words))
			for i, v := range perm {
				r[v] = words[i]
			}
			return strings.Join(r, " ")
		}
	} else {
		dataFunc = func() string {
			return words[0]
		}
	}

	checkpointChannel := make(chan Checkpoint)
	go readCheckpoints(checkpointChannel)
	if len(os.Args) >= 5 {
		// assert message count == -1
		if numMessages != -1 {
			log.Fatal("Can't produce in threads with finite number of messages for now")
		}
		numThreads, err := strconv.Atoi(os.Args[4])
		if err != nil {
			log.Fatal(err)
		}
		for numThreads != 0 {
			go produceMessages(produceChannel, topicPartition, 0, -1, delayDuration, checkpointChannel, string(numThreads), dataFunc)
			numThreads = numThreads - 1
		}
		<-make(chan bool)
	} else if len(config.Topics) > 1 {
		// start one thread per topic
		if numMessages != -1 {
			log.Fatal("Can't produce in threads with finite number of messages for now")
		}
		for _, topic := range config.Topics {
			// assign to tmp var for pointer reasons
			tmp := topic
			go produceMessages(produceChannel, kafka.TopicPartition{Topic: &tmp, Partition: kafka.PartitionAny}, 0, -1, delayDuration, checkpointChannel, topic, dataFunc)
		}
		<-make(chan bool)
	} else if len(config.Topics) == 1 {
		// produce to one topic
		topic := config.Topics[0]
		produceMessages(produceChannel, kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, 0, numMessages, delayDuration, checkpointChannel, topic, dataFunc)
	}
}
