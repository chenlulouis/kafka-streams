# Getting Started

### Reference Documentation

For further reference, please consider the following sections:

* [Official Apache Maven documentation](https://maven.apache.org/guides/index.html)
* [Spring Boot Maven Plugin Reference Guide](https://docs.spring.io/spring-boot/3.5.11/maven-plugin)
* [Create an OCI image](https://docs.spring.io/spring-boot/3.5.11/maven-plugin/build-image.html)
* [Spring for Apache Kafka](https://docs.spring.io/spring-boot/3.5.11/reference/messaging/kafka.html)
* [Apache Kafka Streams Support](https://docs.spring.io/spring-kafka/reference/streams.html)
* [Apache Kafka Streams Binding Capabilities of Spring Cloud Stream](https://docs.spring.io/spring-cloud-stream/reference/kafka/kafka-streams-binder/usage.html)

### Guides

The following guides illustrate how to use some features concretely:

* [Samples for using Apache Kafka Streams with Spring Cloud stream](https://github.com/spring-cloud/spring-cloud-stream-samples/tree/main/kafka-streams-samples)

### Maven Parent overrides

Due to Maven's design, elements are inherited from the parent POM to the project POM. While most of the inheritance is
fine, it also inherits unwanted elements like `<license>` and `<developers>` from the parent. To prevent this, the
project POM contains empty overrides for these elements. If you manually switch to a different parent and actually want
the inheritance, you need to remove those overrides.
------------------------------
PostgreSQL

server-> localhost
user-> postgres
password-> postgres

--------------------------
启动windows spark

spark-shell
:quit / ctrl+d
http://localhost:4040/

cd D:\Open-Source-Code\kafka-streams && d:

%SPARK_HOME%\bin\spark-submit.cmd --class com.example.kafkastreams.SparkStreamingTest --master local[*] .\target\kafka-streams-0.0.1-SNAPSHOT-jar-with-dependencies.jar
--------------------------
启动windows kafka


cd D:\Kafka\kafka_2.13-3.7.0 && d:
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties

.\bin\windows\kafka-server-stop.bat
.\bin\windows\zookeeper-server-stop.bat

.\bin\windows\kafka-topics.bat -create -topic messages -bootstrap-server localhost:9092 -partitions 1 -replication-factor 1
.\bin\windows\kafka-topics.bat -list -bootstrap-server localhost:9092

.\bin\windows\kafka-console-producer.bat -topic messages -bootstrap-server localhost:9092
.\bin\windows\kafka-console-consumer.bat -topic test-topic -bootstrap-server localhost:9092 -from-beginning


.\bin\windows\connect-standalone.bat .\config\connect-standalone.properties .\config\connect-file-source.properties .\config\connect-file-sink.properties

.\bin\windows\kafka-console-producer.bat --bootstrap-server localhost:9092 --topic streams-plaintext-input
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic streams-wordcount-output --from-beginning --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

