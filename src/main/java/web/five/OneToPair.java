package web.five;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author Administrator
 */
public class OneToPair implements Serializable {

    public void oneToPair(JavaSparkContext sparkContext) {
        JavaRDD<String> rdd =
                sparkContext.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "c", "d", "d", "f"));
        PairFunction<String, String, Integer> pairFunction = new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s.split(" ")[0], 1);
            }
        };
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(pairFunction);
        pairRDD.sortByKey(false);
        JavaPairRDD<String, Integer> result = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            // V1和V2分别表达相同key对应的两个值，这里处理是将相同key的元素合并起来，并且把对应的键值相加起来
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> info) throws Exception {
                System.out.println("key:" + info._1 + ",value:" + info._2);
            }
        });
        result.saveAsTextFile("/opt/spark/spark-2.2.0/mydemo/five/pair");
    }
}