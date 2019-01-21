package kryo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.serializer.KryoSerializer;

public class KryoTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("KryoTest");
//        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        conf.set("spark.kryo.registrator", "kryo.MyRegistrator");
//        conf.set("spark.kryo.unsafe", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("D:\\data_test\\test30x3v2.csv");
        JavaRDD<Qualify> map = rdd.map(new Function<String, Qualify>() {
            /* (non-Javadoc)
             * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
             */
            public Qualify call(String v1) throws Exception {
                // TODO Auto-generated method stub
                String s[] =  v1.split(",");
                Qualify q = new Qualify();
                q.setA(Integer.valueOf(s[0]));
                q.setB(Integer.valueOf(s[1]));
                q.setC(s[2]);

                return q;
            }
        });
//        map.cache();
        map.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
        System.out.println(map.count());
        System.out.println(map.count());
        System.out.println("");
    }
}