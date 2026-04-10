package com.example.kafkastreams.transformation.window;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class OperateCountByValueAndWindow {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("countByValueAndWindow");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.checkpoint("checkpoint");
        JavaDStream<String> textFileStream = jsc.textFileStream("data");

        JavaPairDStream<String, Integer> mapToPair = textFileStream.flatMap(new FlatMapFunction<String, String>() {
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
        JavaPairDStream<Tuple2<String, Integer>, Long> countByValueAndWindow = mapToPair.countByValueAndWindow(Durations.seconds(15), Durations.seconds(5));
        countByValueAndWindow.print();

        JavaDStream<Long> countByWindow = mapToPair.countByWindow(Durations.seconds(15), Durations.seconds(5));
        JavaDStream<Long> count = countByWindow.count();
        count.print();

        JavaDStream<String> window2 = textFileStream.window(Durations.seconds(15), Durations.seconds(5));
        window2.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                System.out.println("***************");
            }
        });

        JavaDStream<Tuple2<String, Integer>> reduceByWindow = mapToPair.reduceByWindow(new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
                return new Tuple2<String, Integer>(v1._1+"**"+v2._1, v1._2+v2._2);
            }
        }, Durations.seconds(15), Durations.seconds(5));
        reduceByWindow.print();

        JavaPairDStream<String, Integer> reduceByKeyAndWindow = mapToPair.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(21), Durations.seconds(10));
        reduceByKeyAndWindow.print();

        JavaPairDStream<String, Integer> reduceByKeyAndWindow_1 = mapToPair.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("***********v1*************"+v1);
                System.out.println("***********v2*************"+v2);
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("^^^^^^^^^^^v1^^^^^^^^^^^^^"+v1);
                System.out.println("^^^^^^^^^^^v2^^^^^^^^^^^^^"+v2);

//				return v1-v2-1;
                return v1-v2;
            }
        }, Durations.seconds(20), Durations.seconds(10));
        reduceByKeyAndWindow_1.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
