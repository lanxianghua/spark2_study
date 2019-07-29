package web.three;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by lanxianghua
 * 异常日志统计
 * 运行:/opt/spark/spark-2.2.0/bin/spark-submit --master local[3] --executor-memory 512m --class web.three.SparkApplication  spark-study-client-1.0-SNAPSHOT.jar
 *
 * @author lanxianghua
 * @version 1.0
 * @date 2019/7/18
 */
public class SparkApplication {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("sparkBoot").setMaster("local");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputRDD = sparkContext.textFile("/opt/spark/spark-2.2.0/mydemo/three/20190712-1.log");

        // 使用具体类实现Function
        JavaRDD<String> errorRDD = inputRDD.filter(new ContainsErrorDev("ERROR"));

        JavaRDD<String> warnRDD = inputRDD.filter(new ContainsErrorDev("WARN"));

//        JavaRDD<String> errorRDD = inputRDD.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//                return s.contains("ERROR");
//            }
//        });
//
//        JavaRDD<String> warnRDD = inputRDD.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//                return s.contains("WARN");
//            }
//        });

        JavaRDD<String> badLinesRDD = errorRDD.union(warnRDD);

        long errorRDDCount = errorRDD.count();

        System.out.println("errorRDD 的总数为: " + errorRDDCount);

        for (String rddLine : errorRDD.take(10)) {
            System.out.println("errorRDD的数据是:" + rddLine);
        }

        badLinesRDD.saveAsTextFile("/opt/spark/spark-2.2.0/mydemo/three/data2");
    }
}

class ContainsErrorDev implements Function<String, Boolean> {
    private String query;

    public ContainsErrorDev(String query) {
        this.query = query;
    }

    @Override
    public Boolean call(String v1) {
        return v1.contains(query);
    }
}