package com.example.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class SparkStreamingTest {
    public static void main(String[] args) throws InterruptedException {
        System.out.print("hello world");
        SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("SparkStreamingTest");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(60));
        jsc.sparkContext().setLogLevel("ERROR");
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream_6");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("messages");

        JavaInputDStream<ConsumerRecord<String, String>> lines =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                System.out.println("DStream flatMap.....");
                return Arrays.asList(stringStringConsumerRecord.value().split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        JavaPairDStream<String, Integer> reduceByKey = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        //reduceByKey.print(1000);

        reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                System.out.println("Executed in driver node.......");
                SparkContext context = rdd.context();
                JavaSparkContext javaSparkContext = new JavaSparkContext(context);
                Broadcast<String> broadcast = javaSparkContext.broadcast("hello");
                String value = broadcast.value();
                System.out.println(value);

                JavaPairRDD<String, Integer> mapToPair = rdd.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
                        System.out.println("Executed in the executor of a worker node.......");
                        return new Tuple2<String, Integer>(tuple._1+"~",tuple._2);
                    }
                });

                mapToPair.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        System.out.println(stringIntegerTuple2);
                    }
                });
            }

        });

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
