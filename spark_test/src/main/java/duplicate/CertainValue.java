package duplicate;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 返回指定值的自定义聚合函数
 */
public class CertainValue extends UserDefinedAggregateFunction {
    @Override
    public StructType inputSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("data", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("value", DataTypes.StringType, true));
        return DataTypes.createStructType(structFields);
    }

    @Override
    public StructType bufferSchema() {
        //只需要一个中间参数储存所需值
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("result", DataTypes.StringType, true));
        return DataTypes.createStructType(fields);
    }

    @Override
    public DataType dataType() {
        return DataTypes.StringType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, null);
    }

    //用输入数据input更新buffer值,类似于combineByKey
    @Override
    public void update(MutableAggregationBuffer buffer, Row row) {
        if (buffer.isNullAt(0) || row.getString(1).equals(row.getString(0))) buffer.update(0, row.getString(0));
    }

    @Override
    public void merge(MutableAggregationBuffer buffer, Row row) {
        if (buffer.isNullAt(0)) buffer.update(0, row.getString(0));
    }

    @Override
    public Object evaluate(Row row) {
        return row.getString(0);
    }
}
