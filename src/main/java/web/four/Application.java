package web.four;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Application {

    public static void main(String[] args) {
        rddAvg(new JavaSparkContext());
    }

    public static void rddAvg(JavaSparkContext sparkContext) {
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        RddAvg rddAvg = new RddAvg(0, 0);
        RddAvg result = javaRDD.aggregate(rddAvg, rddAvg.avgFunction2, rddAvg.rddAvgFunction2);
        System.out.println("萤火虫" + result.avg());
    }
}