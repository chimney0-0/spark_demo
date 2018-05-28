package duplicate;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * 返回中位数的自定义聚合函数（evaluate时排序）
 */
public class MiddleValue extends UserDefinedAggregateFunction {

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
        list.add(str);
        buffer.update(0, list);
    }

    //最后在分布式节点进行local reduce完成后需要进行全局级别的merge操作
    @Override
    public void merge(MutableAggregationBuffer buffer, Row row) {
        List<String> list = new ArrayList<>(buffer.getList(0));
        list.addAll(row.getList(0));
        buffer.update(0, list);
    }

    //返回UDAF最后的计算结果
    @Override
    public Object evaluate(Row row) {
        List<String> list = new ArrayList<>(row.getList(0));
        list.sort(comparator);
        return list.get((list.size() - 1) / 2);
    }


}
