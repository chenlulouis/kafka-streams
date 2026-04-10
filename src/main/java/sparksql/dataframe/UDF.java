package sparksql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UDF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("UDF").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        List<String> list = Arrays.asList(
                "a",
                "b",
                "c",
                "dfasd",
                "edasf",
                "fdaf",
                "bdsa",
                "cdsa",
                "b",
                "c");

        JavaRDD<Row> map =jsc.parallelize(list).map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(s);
            }
        });

        List<StructField> structFields= new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        StructType createStructType = DataTypes.createStructType(structFields);

        Dataset<Row> createDataFrame = sqlContext.createDataFrame(map, createStructType);
        createDataFrame.registerTempTable("tab");

        sqlContext.udf().register("StrLen", new UDF1<String, Integer>() {
                    @Override
                    public Integer call(String o) throws Exception {
                        return o.length();
                    }
                }, DataTypes.IntegerType
        );

        sqlContext.sql("select name ,StrLen(name) from tab").show();

        sqlContext.udf().register("fun", new UDF2<String, Integer, String>() {
            @Override
            public String call(String s, Integer c) throws Exception {
                return s+c+"~";
            }
        }, DataTypes.StringType);

        sqlContext.sql("select name ,fun(name,10) from tab").show();
    }
}
