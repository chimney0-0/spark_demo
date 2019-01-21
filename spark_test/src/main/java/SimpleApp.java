import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.Column;

public class SimpleApp {
    public static void main(String[] args) {
        //FIXME
        String logFile = "D:\\spark\\spark-2.3.0-bin-hadoop2.6\\README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().master("local").appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();


        while (true) {
            System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//        spark.stop();
    }
}
