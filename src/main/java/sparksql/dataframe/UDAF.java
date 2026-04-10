package sparksql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UDAF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("UDAF");
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

        JavaRDD<Row> map = jsc.parallelize(list).map(new Function<String, Row>() {
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

        sqlContext.udf().register("CountSameName", new UserDefinedAggregateFunction() {
            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                buffer.update(0, buffer.getInt(0)+1);
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
                buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0));
            }

            @Override
            public Object evaluate(Row row) {
                return row.getInt(0);
            }

            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                buffer.update(0, 0);
            }

            @Override
            public StructType inputSchema() {
                return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("name", DataTypes.StringType, true)));
            }

            @Override
            public StructType bufferSchema() {
                return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("bf", DataTypes.IntegerType, true)));
            }

            @Override
            public DataType dataType() {
                return DataTypes.IntegerType;
            }

            @Override
            public boolean deterministic() {
                return true;
            }
        });

        sqlContext.sql("SELECT name,CountSameName(name) from tab group by name").show();
    }
}
