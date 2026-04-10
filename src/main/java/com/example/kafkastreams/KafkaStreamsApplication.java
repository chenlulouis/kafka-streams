package com.example.kafkastreams;

import org.apache.avro.io.JsonDecoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.kafka.streams.StreamsConfig;
import scala.Tuple2;


import java.sql.DriverManager;
import java.util.*;
import java.util.function.Function;


@SpringBootApplication
public class KafkaStreamsApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsApplication.class, args);
    }


    @Override
    public void run(String... args) throws Exception {
        //Getting JavaStreamingContext
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("WordCountingApp");
        sparkConf.setMaster("local[1]");


        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(6));
        streamingContext.sparkContext().setLogLevel("ERROR");
        //Getting DStream from Kafka
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream_3");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("messages");


        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

        //Processing Obtained DStream
        JavaPairDStream<String, String> results = messages
                .mapToPair(
                        record -> new Tuple2<>(record.key(), record.value())
                );
        JavaDStream<String> lines = results
                .map(
                        tuple2 -> tuple2._2()
                );
        JavaDStream<String> words = lines
                .flatMap(
                        x -> Arrays.asList(x.split("\\s+")).iterator()
                );
        JavaPairDStream<String, Integer> wordCounts = words
                .mapToPair(
                        s -> new Tuple2<>(s, 1)
                ).reduceByKey(
                        (i1, i2) -> i1 + i2
                );


        //Persisting Processed DStream into PostGreSQL
        //wordCounts.print();

        wordCounts.foreachRDD(
                javaRdd -> {
                    Map<String, Integer> wordCountMap = javaRdd.collectAsMap();
                    for (String key : wordCountMap.keySet()) {
                        List<Word> wordList = Arrays.asList(new Word(key, wordCountMap.get(key)));
                        JavaRDD<Word> rdd = streamingContext.sparkContext().parallelize(wordList);

                        rdd.foreach(word -> {
                            Class.forName("org.postgresql.Driver");
                            var connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
                            var statement = connection.prepareStatement("INSERT INTO public.words (word, count) VALUES (?, ?);");
                            // set parameters
                            statement.setString(1, word.getWord());
                            statement.setInt(2, word.getCount());
                            statement.executeUpdate();
                            connection.close();
                        });


                    }
                }
        );

        //Running the Application
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
