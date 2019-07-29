package web.five;

import org.apache.spark.api.java.JavaSparkContext;

public class Application {
    public static void main(String[] args) {
        OneToPair oneToPair = new OneToPair();
        oneToPair.oneToPair(new JavaSparkContext());
    }
}