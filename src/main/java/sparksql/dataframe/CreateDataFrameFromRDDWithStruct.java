package sparksql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class CreateDataFrameFromRDDWithStruct {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CreateDataFrameWithStruct");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        JavaRDD<String> textFile = jsc.textFile("./sparksql/rddfile.txt");

        JavaRDD<Row> map = textFile.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] split = line.split(" ");

                return RowFactory.create(Integer.valueOf(split[0]),split[1],split[2],Integer.valueOf(split[3]));
            }
        });

        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("gender", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(fields);
        Dataset<Row> df = sqlContext.createDataFrame(map, structType);
        df.show();
        df.registerTempTable("structtable");
        Dataset<Row> result = sqlContext.sql("select * from structtable where age < 30");
        result.show();
        result.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row);
            }
        });
    }
}
