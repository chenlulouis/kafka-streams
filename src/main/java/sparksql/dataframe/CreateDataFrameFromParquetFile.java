package sparksql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class CreateDataFrameFromParquetFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CreateDataFrameFromParquetFile");
        SQLContext sqlContext = new SQLContext(new JavaSparkContext(conf));

//        Dataset<Row> df = sqlContext.read().format("json").load("./sparksql/json.txt");
//        df.write().format("parquet").mode("overwrite").save("./sparksql/parquet.parquet");
//        df.write().mode("overwrite").parquet("./sparksql/parquet.parquet");

//        Dataset<Row> parquetdf = sqlContext.read().format("parquet").load(".\\sparksql\\parquet.parquet");
//        parquetdf.show();
//        parquetdf.registerTempTable("parquet");
//        sqlContext.sql("Select * from parquet where age > 19").show();

        sqlContext.sql("select * from parquet.`./sparksql/parquet.parquet` as t where t.name = 'zhangsan'").show();
    }
}
