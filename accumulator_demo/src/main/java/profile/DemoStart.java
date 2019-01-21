package profile;

import com.google.gson.Gson;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;

public class DemoStart {

	public static void main(String[] args) {

		Long current = System.currentTimeMillis();

		SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.driver.maxResultSize", "10G").getOrCreate();

		DemoAccumulator accumulator = new DemoAccumulator();

		spark.sparkContext().register(accumulator);

		// 处理的数据
		String path = args[0];
		Dataset<Row> dataset = spark.read().csv(path);
		Long count = dataset.toJavaRDD().map(f -> {
			for (int i = 0; i < f.size(); i++) {
				accumulator.add(f.get(i));
			}
			return f;
		}).count();

		System.out.println(count);

		Map<String, Integer> map = (Map<String, Integer>) accumulator.value();
		System.out.println(map.size());

		System.out.println("耗时" + (System.currentTimeMillis() - current) + "ms");

		System.out.println("程序3分钟后关闭");

		try {
			Thread.sleep(1000 * 60 * 3);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

//		dataset.show();

		System.out.println("程序关闭");
	}

}
