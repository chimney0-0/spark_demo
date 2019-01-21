package duplicate;

import org.apache.spark.sql.sources.In;
import scala.Int;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BinaryInsertTest {

    static public void update(List<Integer> list, Integer integer){
        if (list.size() > 0) {
            //二分插入
            int start = 0;
            int end = list.size() - 1;
            int i = (start + end) / 2;
            while (start < end) {
                int cmp = Integer.compare(list.get(i), integer);
                if (cmp == 0) break;
                if (cmp > 0) end = i-1;
                else start = i+1;
                i = (start + end) >> 1;
            }
            list.add(i+1, integer);
        } else {
            list.add(integer);
        }
        System.out.println(list);
    }


    public static void main(String[] args) {
        Integer[] integers = new Integer[]{1,4,6,7,10};
        Integer integer = 0;
        update(new ArrayList<>(Arrays.asList(integers)), integer);

    }



}
