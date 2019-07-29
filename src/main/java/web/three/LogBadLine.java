package web.three;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

/**
 * 清洗日志文件，保留ERROR和WARN两种日志的内容
 *
 * @author lanxianghua
 * @date 2019/07/24
 */
public class LogBadLine implements Serializable {

    /**
     * 对日志进行 转换操作和行动操作
     */
    public void log(JavaSparkContext sparkContext) {
        JavaRDD<String> inputRDD = sparkContext.textFile("/opt/spark/spark-2.2.0/mydemo/three/logdir");
        JavaRDD<String> errorRDD = inputRDD.filter(new ContainsErrorDev("ERROR"));
        JavaRDD<String> warnRDD = inputRDD.filter(new ContainsErrorDev("WARN"));

        long errorRDDCount = errorRDD.count();
        long warnRDDCount = warnRDD.count();
        System.out.println("errorRDD count is " + errorRDDCount);
        System.out.println("warnRDD count is " + warnRDDCount);
        for (String rddLine : errorRDD.take(10)) {
            System.out.println("errorRDD 数据is " + rddLine);
        }
        for (String rddLine : warnRDD.take(10)) {
            System.out.println("warnRDD 数据is " + rddLine);
        }
        errorRDD.saveAsTextFile("/opt/spark/spark-2.2.0/mydemo/three/error");
        warnRDD.saveAsTextFile("/opt/spark/spark-2.2.0/mydemo/three/warn");
    }

    /**
     * 对 ContainsError 具体类改造,改的更加灵活
     */
    class ContainsErrorDev implements Function<String, Boolean> {
        private String query;

        public ContainsErrorDev(String query) {
            this.query = query;
        }

        @Override public Boolean call(String v1) {
            return v1.contains(query);
        }
    }
}