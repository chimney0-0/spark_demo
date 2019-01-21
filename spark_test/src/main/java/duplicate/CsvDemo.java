package duplicate;

import com.univocity.parsers.csv.Csv;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sha1;

public class CsvDemo {
    static Dataset<Row> dataSet;


    public static void main(String[] args) throws AnalysisException {
//        System.out.println(new BigDecimal("-0.12").compareTo(new BigDecimal("-0.11")));

//        List<String> list = new ArrayList<>();
//        list.sort(Comparator.naturalOrder());

//        System.out.println(isNumeric("234") + " " + isNumeric("233.43") + " " + isNumeric("-21") + " " + isNumeric("-0.534"));


        SparkSession spark = SparkSession.builder().master("local[2]").appName("csv").getOrCreate();
//        dataSet = spark.read().option("header", true).csv("E:\\data_test\\big.csv");
        dataSet = spark.read().option("header", true).csv("E:\\data_test\\test10x10.csv");
//        System.out.println(dataSet.dropDuplicates("_c1", "_c2").count());

//        dataSet.createTempView("a");

//        dataSet.sparkSession().sql("select avg(c3) from a").show();

//        spark.udf().register("certain", new CertainValue());
//        spark.sql("select c1, c2, certain(c3, 'd') from a group by c1, c2").show();

//        spark.udf().register("middle", new MiddleValue());
//        spark.udf().register("middle", new MiddleValue());
//        spark.udf().register("middle", new MiddleValue());
//        spark.sql("select c1, c2, middle(c5) from a group by c1, c2").show();

//        dataSet.sparkSession().sql("select * from a where c2 = 1").createTempView("b");
//        dataSet.sparkSession().sql("select * from b,a where b.c2 = a.c2").show();


        //测试去重类
        duplicate.DeduplicateDemo deduplicateDemo = new duplicate.DeduplicateDemo();
//        System.out.println(deduplicateDemo.countDuplicates(dataSet,"c1", "c2"));

//        deduplicateDemo.dropDuplicatesRandow(dataSet, "c1", "c2").show();

//        deduplicateDemo.dropDuplicatesMax(dataSet, "c3", "c1", "c2").show();
//        deduplicateDemo.dropDuplicatesMin(dataSet, "c3", "c1", "c2").show();
        Long date1 = System.currentTimeMillis();
//        deduplicateDemo.dropDuplicatesMiddle(dataSet, "c5", "c1", "c2").show();
        deduplicateDemo.dropDuplicatesMiddle(dataSet, "_c5", "_c1", "_c2").show();
        Long date2 = System.currentTimeMillis();

        Long date3 = System.currentTimeMillis();
//        deduplicateDemo.dropDuplicatesCertain(dataSet, "c5", "999", "c1", "c2").show();
        deduplicateDemo.dropDuplicatesCertain(dataSet, "_c5", "oW", "_c1", "_c2").show();
        Long date4 = System.currentTimeMillis();

        System.out.println("计算中位数毫秒数："+(date2 - date1));
        System.out.println("计算指定值毫秒数："+(date4 - date3));

//        dataSet.dropDuplicates("c1", "c2").show();

//        dataSet.groupBy("c1", "c2").min("c3").show(); //c3非数值则报错

//        dataSet.groupBy("c1", "c2").min("c4").show();

//        dataSet.createTempView("small");
//
//       spark.sql("select c1, c2, min(c3) from small group by c1, c2").show();

//        dataSet.show();


//        dataSet.show();

//        dataSet.dropDuplicates("_c1").show();

//        dataSet.col("_c1").as("ccc");

//        dataSet.select(col("_c0"),
//                col("_c1"),
//                col("_c1").like("AB%"),
//                col("_c1").rlike("AB.*"))
//                .show(10);
//
//        dataSet.filter(col("_c0").startsWith(col("_c1"))).show(10);
//
//        dataSet.select(col("_c0")).sort(col("_c0").desc()) .show();


    }

}
