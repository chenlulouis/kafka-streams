package sparksql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.List;

public class CreateDataFrameFromJsonRDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CreateDataFrameFromJsonRDD");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(jsc);

        List<String> asList = Arrays.asList(
                "{\"name\":\"zhansan\",\"age\":1}",
                "{\"name\":\"lisi\"}",
                "{\"name\":\"wangwu\"}"
        );
        JavaRDD<String> jsonRDD = jsc.parallelize(asList);

        Dataset<Row> df = sqlContext.read().json(jsonRDD);

        df.show();
        df.registerTempTable("jsonRDDtable");
        sqlContext.sql("select * from jsonRDDtable").show();
    }
}
