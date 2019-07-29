package web.ten.dataset;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Collections;

public class DataSetApplication {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local")
                .appName("Java Spark SQL")
                .getOrCreate();
        Person person = new Person("spark", 10);
        // DataSet是使用的encoder来实现对象的序列化和在网络中的传输
        Encoder<Person> encoder = Encoders.bean(Person.class);
        Dataset<Person> dataset = sparkSession.createDataset(Collections.singletonList(person), encoder);
        dataset.show();
        // 最终输出 {name:spark;age:10}


        /*常见类型的编码器*/
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> integerDataset = sparkSession.createDataset(Arrays.asList(1, 2), integerEncoder);
        Dataset<Integer> result = integerDataset.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer call(Integer value) {
                return value + 1;
            }
        }, integerEncoder);
        result.collect();
        // 最终输出 [2,3]
        /*通过提供一个类，可以将数据流转换为数据集。基于名称的映射*/
        String url = "/opt/spark/spark-2.2.0/mydemo/ten/a.json";
        Dataset<Person> personDataset = sparkSession.read().json(url).as(encoder);
        personDataset.show();
        // 最终输出 name:...  age:,,,,
    }
}
