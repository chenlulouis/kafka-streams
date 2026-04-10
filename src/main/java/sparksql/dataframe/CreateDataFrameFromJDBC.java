package sparksql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

public class CreateDataFrameFromJDBC {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CreateDataFrameFromJDBC");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        jsc.setLogLevel("OFF");
/*        Map<String, String> options = new HashMap<String,String>();
        options.put("url", "jdbc:postgresql://localhost:5432/postgres");
        options.put("driver", "org.postgresql.Driver");
        options.put("user","postgres");
        options.put("password", "postgres");
        options.put("dbtable", "words");
        Dataset<Row> load = sqlContext.read().format("jdbc").options(options).load();

        load.registerTempTable("words_view");

        Dataset<Row> sql = sqlContext.sql("select * from words_view");
        sql.show();*/



        DataFrameReader reader = sqlContext.read().format("jdbc");
		reader.option("url", "jdbc:postgresql://localhost:5432/postgres");
		reader.option("driver", "org.postgresql.Driver");
		reader.option("user", "postgres");
		reader.option("password", "postgres");
		reader.option("dbtable", "words");
        Dataset<Row> load3 = reader.load();
		load3.registerTempTable("words_view_1");


		sqlContext.sql("select * from words_view_1").show();
    }
}
