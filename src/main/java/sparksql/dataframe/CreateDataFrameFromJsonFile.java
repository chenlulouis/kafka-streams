package sparksql.dataframe;

import org.apache.avro.data.Json;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class CreateDataFrameFromJsonFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CreateDataFrameFromJsonFile");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        Dataset<Row> df = sqlContext.read().format("json").load(".\\sparksql\\json.txt");

//    Dataset<Row> df = sqlContext.read().json("./sparksql/json.txt");
        df.show();
        df.printSchema();
        df.select("name").show();

        df.select(df.col("name"), df.col("age").plus(1).as("newage")).show();
        df.printSchema();

        df.filter(df.col("age").plus(1).gt("19")).show();

        df.groupBy("age").count().show();

        System.out.println("--------------------------------------------------------------");

        df.registerTempTable("dftable");

        Dataset<Row> sql = sqlContext.sql("SELECT * from dftable");
        sql.show();

        sqlContext.sql("SELECT * FROM dftable t1 join dftable t2 ON t1.name = t2.name").show();
    }


}
