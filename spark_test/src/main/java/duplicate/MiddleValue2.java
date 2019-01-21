package duplicate;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 返回中位数的自定义聚合函数（使用二分插入排序）
 */
public class MiddleValue2 extends UserDefinedAggregateFunction {

    static private MiddleComparator comparator = new MiddleComparator();

    //输入的数据类型
    @Override
    public StructType inputSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("data", DataTypes.StringType, true));
        return DataTypes.createStructType(structFields);
    }

    //中间聚合处理时，需要处理的数据类型
    @Override
    public StructType bufferSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("list", DataTypes.createArrayType(DataTypes.StringType), true));
        return DataTypes.createStructType(structFields);
    }

    //函数的返回类型
    @Override
    public DataType dataType() {
        return DataTypes.StringType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    //为每个分组的数据初始化
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, new ArrayList());
    }

    //每个分组，有新的值进来时，如何进行分组的聚合计算
    @Override
    public void update(MutableAggregationBuffer buffer, Row row) {
        List<String> list = new ArrayList<>(buffer.getList(0));
        String str = row.getString(0);
        if (list.size() > 0) {
            //二分插入
            int start = 0;
            int end = list.size() - 1;
            int i = (start + end) >> 1;
            while (start < end) {
                int cmp = comparator.compare(list.get(i), str);
                if (cmp == 0) break;
                if (cmp > 0) end = i - 1;
                else start = i + 1;
                i = (start + end) >> 1;
            }
            list.add(i + 1, str);
        } else {
            list.add(str);
        }
        buffer.update(0, list);
    }

    //最后在分布式节点进行local reduce完成后需要进行全局级别的merge操作
    @Override
    public void merge(MutableAggregationBuffer buffer, Row row) {
        //FIXME 两个列表归并排序
        List<String> a = buffer.getList(0);
        List<String> b = row.getList(0);
        String[] list = new String[a.size() + b.size()];
        int lenA = 0, lenB = 0, lenMer = 0;
        while (lenA < a.size() && lenB < b.size()) {
            if (comparator.compare(a.get(lenA), b.get(lenB)) < 0) {
                list[lenMer++] = a.get(lenA++);
            } else {
                list[lenMer++] = b.get(lenB++);
            }
        }
        while (lenA < a.size()) {
            list[lenMer++] = a.get(lenA++);
        }

        while (lenB < b.size()) {
            list[lenMer++] = b.get(lenB++);
        }

//        list.addAll(row.getList(0));
        buffer.update(0, Arrays.asList(list));
    }

    //返回UDAF最后的计算结果
    @Override
    public Object evaluate(Row row) {
//        List<String> list = new ArrayList<>(row.getList(0));
//        list.sort(new MiddleComparator());
        List<String> list = row.getList(0);
        return list.get((list.size() - 1) / 2);
    }


}
