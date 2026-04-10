package com.example.kafkastreams.transformation;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public class SQLWithStreaming {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("WindowOnStreaming");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.sparkContext().setLogLevel("ERROR");
        jsc.checkpoint("./checkpoint");
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream_12");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList("messages");

        JavaInputDStream<ConsumerRecord<String, String>> lines =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> lines) throws Exception {
                return Arrays.asList(lines.value().split(" ")).iterator();
            }
        });

        JavaDStream<String> windowWords = words.window(Durations.minutes(1), Durations.seconds(15));

        windowWords.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                JavaRDD<Row> wordRowRDD = rdd.map(new Function<String, Row>() {
                    @Override
                    public Row call(String word) throws Exception {
                        return RowFactory.create(word);
                    }
                });

                SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
                List<StructField> fields = new ArrayList<StructField>();
                fields.add(DataTypes.createStructField("word", DataTypes.StringType, true));
                StructType createStructType = DataTypes.createStructType(fields);
                Dataset<Row> allwords= sqlContext.createDataFrame(wordRowRDD, createStructType);
                //allwords.show();
                allwords.registerTempTable("words");
                Dataset<Row> result = sqlContext.sql("select word , count(*) rank from words group by word order by rank");
                result.printSchema();
                result.show(10);

            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
