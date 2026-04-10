package com.example.kafkastreams.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class OperateCogroup {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("operate_cogroup");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.sparkContext().setLogLevel("FATAL");
        JavaDStream<String> textFileStream = jsc.textFileStream("data");

        JavaPairDStream<String, Integer> mapToPair = textFileStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String lines) throws Exception {
                return new Tuple2<String, Integer>(lines.split(" ")[0].trim(), Integer.valueOf(lines.split(" ")[1].trim()));
            }
        });
        System.out.print("operate_cogroup");
        mapToPair.cogroup(mapToPair).print();

        JavaDStream<Long> count = textFileStream.count();
        System.out.print("operate_count");
        count.print();

        JavaPairDStream<Tuple2<String, Integer>, Long> countByValue = mapToPair.countByValue();
        countByValue.print(1000);

        textFileStream.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                return line.startsWith("a 100");
            }
        }).print(1000);

        JavaDStream<String> flatMap = textFileStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        });
        flatMap.print(1000);

        JavaPairDStream<String, Integer> flatMapToPair = textFileStream.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String t) throws Exception {
                return Arrays.asList(new Tuple2<String , Integer>(t.trim(), 1)).iterator();
            }
        });
        flatMapToPair.join(flatMapToPair).print(1000);

        JavaDStream<String> map = textFileStream.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s.trim();
            }
        });
        map.print(1000);

        JavaDStream<String> reduce = textFileStream.reduce(new Function2<String, String, String>() {
            @Override
            public String call(String s1, String s2) throws Exception {
                return s1 + "****" + s2;
            }
        });
        reduce.print(1000);

        flatMapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).print(1000);

        JavaDStream<String> repartition = textFileStream.repartition(8);
        repartition.print(1000);
        repartition.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                System.out.println("rdd partition is "+rdd.partitions().size());
            }
        });

        textFileStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {

                rdd.foreach(new VoidFunction<String>() {

                    public void call(String t) throws Exception {
                        System.err.println("**************"+t);
                    }

                });

                return rdd;
            }
        }).print();

        JavaDStream<String> textFileStream2 = jsc.textFileStream("data");
        JavaDStream<String> union = textFileStream.union(textFileStream2);
        union.print(1000);

        jsc.checkpoint("checkpoint");
        JavaPairDStream<String, Integer> mapToPair_1 = textFileStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2<String, Integer>(t.trim(), 1);
            }
        });
        JavaPairDStream<String, Integer> updateStateByKey = mapToPair_1.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer updateValue = 0;
                if(state.isPresent()){
                    updateValue = state.get();
                }
                for(Integer i : values){
                    updateValue += i;
                }
                return Optional.of(updateValue);
            }
        });
        updateStateByKey.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
