package web.five;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {

    public static void wordCountObject(JavaSparkContext sparkContext) {
        JavaRDD<String> wordCount = sparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "1", "5"));

        //返回一个可以迭代的集合
        JavaRDD<String> c = wordCount.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String v1) throws Exception {
                return Arrays.asList(v1.split(" ")).iterator();
            }
        });

        //现在的数据是 1,2,3,4,5
        JavaPairRDD<String, Integer> result = c.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
                //此时的数据是:[{1,1},{2,1},{3,1},{4,1},{5,1},{1,1},{5,1}]
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
                // 此时的数据是:[(3,1), (4,1), (1,2), (5,2), (2,1)]
            }
        });
        System.out.println(result.collect().toString());
    }

    public static void main(String[] args) {
        wordCountObject(new JavaSparkContext());
    }

}
