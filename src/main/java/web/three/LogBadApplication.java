package web.three;

import org.apache.spark.api.java.JavaSparkContext;

public class LogBadApplication {
    public static void main(String[] args) {
        LogBadLine logBadLine = new LogBadLine();
        logBadLine.log(new JavaSparkContext());
    }
}
