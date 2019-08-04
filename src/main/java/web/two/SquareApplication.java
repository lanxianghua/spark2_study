package web.two;

import cn.hutool.core.collection.CollUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import java.util.Arrays;
import java.util.List;

/**
 * SquareApplication : 简单的rdd操作例子
 * 
 * @author : lanxianghua
 * @version : V0.1
 * @date : 2019/07/10
 */
public class SquareApplication {
    // 创建一个驱动器程序,驱动器程序包含了应用的main函数,并且定义了集群上的分布式数据集,并对这些数据集进行相关的操作
    public static void main(String[] args) {
        // 初始化SparkContext,需要两个参数{集群URL,应用名}
        SparkConf sparkConf = new SparkConf().setAppName("SquareCalc").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        // 在驱动器程序中分发驱动器程序中的集合
        JavaRDD<Integer> srcRddData = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<String> destRddData = srcRddData.map(new Function<Integer, String>() {
            @Override
            public String call(Integer v1) throws Exception {
                return String.valueOf(v1 * v1);
            }
        });
        List<Integer> srcDataList = srcRddData.collect();
        if (CollUtil.isNotEmpty(srcDataList)) {
            System.out.println("输入的集合为:");
            for (Integer num : srcDataList) {
                System.out.println(num);
            }
        }
        List<String> resultList = destRddData.collect();
        if (CollUtil.isNotEmpty(resultList)) {
            System.out.println("输出的集合为:");
            for (String num : resultList) {
                System.out.println(num);
            }
        }
    }
}