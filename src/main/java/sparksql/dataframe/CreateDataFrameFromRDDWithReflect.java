package sparksql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class CreateDataFrameFromRDDWithReflect {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CreateDataFrameFromRDD");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        JavaRDD<String> textFile = jsc.textFile("./sparksql/rddfile.txt");
        JavaRDD<Person> map = textFile.map(new Function<String, Person>() {
            @Override
            public Person call(String line) throws Exception {
                String[] split = line.split(" ");
                Person person = new Person();
                person.setId(Integer.valueOf(split[0]));
                person.setName(split[1]);
                person.setGender(split[2]);
                person.setAge(Integer.valueOf(split[3]));
                return person;
            }
        });

        Dataset<Row> df = sqlContext.createDataFrame(map, Person.class);
        df.registerTempTable("person");
        Dataset<Row> sql = sqlContext.sql("select name from person where id >2");
        sql.show();
        JavaRDD<Row> rdd = df.javaRDD();

        rdd.map(new Function<Row, Person>() {
            @Override
            public Person call(Row row) throws Exception {
                Integer id = row.getAs("id");
                String name = row.getAs("name");
                String gender = row.getAs("gender");
                Integer age = row.getAs("age");
                Person person = new Person(id,name,gender,age);
                return person;
            }
        }).foreach(new VoidFunction<Person>() {
            @Override
            public void call(Person person) throws Exception {
                System.out.println(person.toString());
            }
        });
    }
}
