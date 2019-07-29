package web.four;

import cn.hutool.core.util.StrUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by lanxianghua wordcount案例测试
 *
 * @author lanxianghua
 * @version 1.0
 * @date 2019/7/18
 */
public class MapApplication {

    private static final Pattern PATTERN = Pattern.compile(" ");

    /**
     * 计算RDD中各对象的属性
     */
    public static void mapObject(JavaSparkContext sparkContext) {
        Student sOne = new Student();
        sOne.setId(1000L);
        sOne.setAge(20);
        sOne.setName("蓝享华");
        Student sTwo = new Student();
        sTwo.setId(2000L);
        sTwo.setAge(3);
        sTwo.setName("蓝文彬");
        List<Student> studentList = new ArrayList<Student>();
        studentList.add(sOne);
        studentList.add(sTwo);
        JavaRDD<Student> studentRDD = sparkContext.parallelize(studentList);

        // 新生成的RDD元素
        JavaRDD<Student> result = studentRDD.map(new Function<Student, Student>() {
            @Override
            public Student call(Student v1) throws Exception {
                v1.setAge(v1.getAge() * 2);
                v1.setName("您好:" + v1.getName());
                v1.setId(v1.getId() * 1000L);
                return v1;
            }
        });
        System.out.println("新的学生集合RDD--开始");
        for (Student student : result.collect()) {
            System.out.println(student.getId() + "|" + student.getName() + "|" + student.getAge());
        }
        System.out.println("新的学生集合RDD--完成");
    }

    /**
     * 计算RDD中各值的平方
     */
    public static void map(JavaSparkContext sparkContext) {
        JavaRDD<Integer> num = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));

        // 新生成的RDD元素
        JavaRDD<Integer> result = num.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * v1;
            }
        });
        System.out.println(StrUtil.join(",", result.collect()));
    }

    /**
     * flatMap分割字符串
     */
    public static void flatMap(JavaSparkContext sparkContext) {
        JavaRDD<String> lines = sparkContext.parallelize(Arrays.asList("hello world", "hi"));

        JavaRDD<String> flatMapResult = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(PATTERN.split(s)).iterator();
            }
        });
        flatMapResult.first();
        // 结果:hello
    }

    public static void reduce(JavaSparkContext sparkContext) {
        JavaRDD<Integer> lines = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> toLines = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4));
    }

    public static void main(String[] args) {
        mapObject(new JavaSparkContext());
    }
}