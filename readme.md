# Library: Kafka Consumer
## Overview   
  
A Java library that gets smart meter readings from a Kafka topic, creates 5 min container files and then writes to HDFS as csv files.

## Requirements
See [POM file](./pom.xml)


## Usage Example
```java
java -cp smartmeter-kafka-consumer-0.0.1-jar-with-dependencies.jar com.khattak.bigdata.realtime.sensordataanalytics.smartmeter.SmartMeterEventsConsumer 192.168.70.136:9092 smartmeter-readings smartmeter-consumer-gp-001 spark-consumer
```

## License
The content of this project is licensed under the [Creative Commons Attribution 3.0 license](https://creativecommons.org/licenses/by/3.0/us/deed.en_US).