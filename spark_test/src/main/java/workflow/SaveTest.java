package workflow;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.scheduler.DAGSchedulerEventProcessLoop;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SaveTest {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[4]").appName("Simple Application").getOrCreate();

        // VLP, VClass, count

        Dataset<Row> dataset = spark.read().option("header", "true").csv("D:\\data_test\\profile\\profile_test100wX10.csv");

        dataset.show();

//        dataset.sample(0.001).show();


//        String url = "jdbc:oracle:thin:@" + "10.50.12.13" + ":" + "1521" + ":"+"ORCL";
//        Properties properties = new Properties();
//        properties.setProperty("user", "c##test");
//        properties.setProperty("password", "orcl123456");
//        dataset.write().mode(SaveMode.Overwrite).jdbc(url, "test1026", properties);


//        dataset.show();
//
//        Dataset<Row> result = dataset.groupBy("VLP", "VClass").count();
//
//        result.write().option("header", "true").csv("D:\\项目资料\\随巢\\gaosu\\result");


        //

//        Dataset<Row> dataset = spark.read().option("header", "true").csv("D:\\项目资料\\随巢\\gaosu\\result");
//
//        dataset.show();
//
//        dataset.createOrReplaceTempView("origin");

//        System.out.println(" ====== rows: "+dataset.count());

//        dataset.show();
//
//        Dataset<Row> group = dataset.groupBy("VLP").count();
//
//        group.createOrReplaceTempView("group");
//
//        group.show();

//        Dataset<Row> unique = spark.sql("select origin.* from origin, group where origin.VLP = group.VLP and group.count = 1");
//
//        unique.show();
//
//        unique.write().option("header", "true").csv("D:\\项目资料\\随巢\\gaosu\\unique");

//        Dataset<Row> notunique = spark.sql("select origin.* from origin, group where origin.VLP = group.VLP and group.count > 1");
//
//        notunique.show();
//
//        notunique.write().option("header", "true").csv("D:\\项目资料\\随巢\\gaosu\\notunique");
//
//        group.show();

//        dataset = spark.read().option("header", "true").csv("D:\\项目资料\\随巢\\gaosu\\unique");
//
//        System.out.println(" ====== rows: "+dataset.count());
//
//        dataset = spark.read().option("header", "true").csv("D:\\项目资料\\随巢\\gaosu\\notunique");
//
//        System.out.println(" ====== rows: "+dataset.count());

    }


}
