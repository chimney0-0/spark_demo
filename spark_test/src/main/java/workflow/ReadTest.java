package workflow;

import com.google.common.base.Joiner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.desc;

public class ReadTest {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[4]").appName("Simple Application").config("spark.driver.maxResultSize", "10G").getOrCreate();

		Dataset<Row> df = spark.read()
				.option("header", false)
				.option("encoding", "utf-8")
				//.option("escape","\\")
				.csv("hdfs://suichao1:8020/user/root/suichao/编目数据/原始数据/industry_classification");

		df.show();

//		dataset.map(f -> f.get(0))

//		dataset = dataset.filter(row -> row.isNullAt(4) && row.getString(4).equals("2018/8/21 19:18:51"));

//		dataset = dataset.sample(0.01);
//		dataset.show();

//		String columnName = "东经.西经";
//		df.select(columnName).show();

//		StructField[] fields = df.schema().fields();
//		List<Integer> indexes = new ArrayList<>();
//		for (int i = 0; i < fields.length; i++) {
//			indexes.add(i);
//		}
//		for (Integer index : indexes) {
//			StructField field = fields[index];
//			String columnName = field.name();
//			columnName = "`"+columnName+"`";
//			Dataset<Row> dataset = df.select(columnName);
//			dataset = dataset.groupBy(columnName).count().orderBy(desc("count"), desc(columnName));
//			dataset = dataset.limit(2000);
//			dataset.show();
//		}

//		System.out.println("==="+dataset.count());

//		System.out.println("===ResourceID: "+dataset.select("ResourceID").distinct().count());
//		System.out.println("===cpu_usage: "+dataset.select("cpu_usage").distinct().count());
//		System.out.println("===Time: "+dataset.select("Time").distinct().count());

//		System.out.println("===ResourceID: "+dataset.select("ResourceID").toJavaRDD().countApproxDistinct(0.05));
//		System.out.println("===cpu_usage: "+dataset.select("cpu_usage").toJavaRDD().countApproxDistinct(0.05));
//		System.out.println("===Time: "+dataset.select("Time").toJavaRDD().countApproxDistinct(0.05));


//		System.out.println(dataset.limit(4).select("_c0").collectAsList().size());

		/*
		dataset = dataset.filter(row -> !row.getString(3).startsWith("db_"));

		List<Row> list = dataset.collectAsList();

		list.forEach(row -> {
			System.out.println(row.get(0)+" "+row.get(1)+" "+row.get(2)+" "+row.get(3)+" "+row.get(4));
		});

		System.out.println(list.size());*/

//		dataset = dataset.drop("name","age","vin","a1","a2","a3");
//
//		dataset.write().option("header", true).csv("D:\\data_test\\CESI2");
//


//		try {
//			Thread.sleep(1000 * 60);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//
//		dataset = spark.read().option("header", "true").csv("D:\\data_test\\date.csv");
//
//		dataset.show();

//		StructType structType = new StructType();
//		structType = structType.add("c1", DataTypes.StringType);
//		structType = structType.add("c2", DataTypes.StringType);
//
//		List<Row> list = new ArrayList<>();
//		Object[] data = {"a", "b"};
//		Row row = new GenericRow(data);
//		list.add(row);
//
//		spark.createDataFrame(list, structType).show();


	}

}
