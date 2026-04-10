package com.example.kafkastreams.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class StreamingBroadcastAccumulator {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("StreamingBroadcastAccumulator");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        List<String> asList = Arrays.asList("a","b");
        final Broadcast<List<String>> blackList = jsc.sparkContext().broadcast(asList);
        JavaDStream<String> textFileStream = jsc.textFileStream("./data");
        JavaPairDStream<String, Integer> maptopair = textFileStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.trim(), 1);
            }
        });

        maptopair.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> pairRdd) throws Exception {
                final List<String> black = blackList.getValue();
                if(!pairRdd.isEmpty()){
                    pairRdd.foreach(new VoidFunction<Tuple2<String,Integer>>() {
                        public void call(Tuple2<String, Integer> tuple)throws Exception {
                            if(black.contains(tuple._1)){

                            }
                        }
                    });


                }
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
