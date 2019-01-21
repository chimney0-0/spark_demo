package workflow;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.EmptyRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.reflect.ClassTypeManifest;

import java.util.ArrayList;
import java.util.List;

public class CreateEmptyTest {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[6]").appName("Simple Application").getOrCreate();

		StructType structType = new StructType();
		structType = structType.add("aa", DataTypes.StringType);
		structType = structType.add("bb", DataTypes.StringType);

		JavaRDD javaRDD = null;
//		RDD<Row> rdd = new EmptyRDD<Row>(spark, );

		List<Row> list = new ArrayList<>();

		Dataset<Row> dataset = spark.createDataFrame(list, structType);

		dataset.show();

//		Dataset<Row> dataset = spark.read().option("header","true").csv("D:\\data_test\\emptydata.csv");
//
//		dataset.show();



	}

}
