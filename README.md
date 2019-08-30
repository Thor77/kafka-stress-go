kafka-stress-go
===============
Produce messages with a given interval to a given topic (in multiple threads).

# Building
`go build -tags static`

# Usage
```
$ ./kafka-stress-go <configfile> <num messages, -1 for infinite> <delay between messages> [<num threads>]
```

### Example
```
$ ./kafka-stress-go config.json -1 1ms 5
```

# Configuration
| key | description |
|-|-|
| topic | topic to produce to |
| topics | multiple topics to produce to, will spawn one thread per topic and ignore `<num threads>` |
| checkpoint_interval | number of messages to print metrics after |
| word_count | number of words in data string (not setting this will result in a non-separated random string) |
| word_length | length of randomly generated words or random string |
| producer | [librdkafka configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) |

### Example
```json
{
  "topic": "topic01",
  "checkpoint_interval": 1000,
  "producer": {
    "bootstrap.servers": "kafka01:9092,kafka02:9092,kafka03:9092"
  }
}
```
