package duplicate;

import com.google.common.base.Joiner;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DeduplicateDemo {

    /**
     * 计算重复数量
     *
     * @param dataSet
     * @param columns
     * @return
     */
    public Long countDuplicates(Dataset<Row> dataSet, String... columns) {
        return dataSet.count() - dataSet.dropDuplicates(columns).count();
    }

    /**
     * 去重：随机保留
     *
     * @param dataSet
     * @param columns
     * @return
     */
    public Dataset<Row> dropDuplicatesRandow(Dataset<Row> dataSet, String... columns) {
        //FIXME 排序怎么说?
        return dataSet.dropDuplicates(columns);
    }

    /**
     * 去重：保留最大值
     *
     * @param dataSet
     * @param rankColumn
     * @param columns
     * @return
     * @throws AnalysisException
     */
    public Dataset<Row> dropDuplicatesMax(Dataset<Row> dataSet, String rankColumn, String... columns) throws AnalysisException {
        dataSet.createOrReplaceTempView("origin");
        String rankSql = filterSql("origin", "max", rankColumn, columns);
        dataSet.sparkSession().sql(rankSql).createOrReplaceTempView("a");
        List<String> strings = new ArrayList<>(Arrays.asList(columns));
        strings.add(rankColumn);
        String[] restrictColumns = strings.toArray(new String[strings.size()]);
        Dataset<Row> found = dataSet.sparkSession().sql(joinSql("origin", "a", restrictColumns));
        //在rankColumn和columns的限制下也有可能重复
        return dropDuplicatesRandow(found, restrictColumns);
    }

    /**
     * 去重：保留最小值
     *
     * @param dataSet
     * @param rankColumn
     * @param columns
     * @return
     * @throws AnalysisException
     */
    public Dataset<Row> dropDuplicatesMin(Dataset<Row> dataSet, String rankColumn, String... columns) throws AnalysisException {
        dataSet.createOrReplaceTempView("origin");
        String rankSql = filterSql("origin", "min", rankColumn, columns);
        dataSet.sparkSession().sql(rankSql).createOrReplaceTempView("a");
        List<String> strings = new ArrayList<>(Arrays.asList(columns));
        strings.add(rankColumn);
        String[] restrictColumns = strings.toArray(new String[strings.size()]);
        Dataset<Row> found = dataSet.sparkSession().sql(joinSql("origin", "a", restrictColumns));
        //在rankColumn和columns的限制下也有可能重复
        return dropDuplicatesRandow(found, restrictColumns);
    }

    /**
     * 去重：保留中位数
     *
     * @param dataSet
     * @param rankColumn
     * @param columns
     * @return
     */
    public Dataset<Row> dropDuplicatesMiddle(Dataset<Row> dataSet, String rankColumn, String... columns) {
        dataSet.createOrReplaceTempView("origin");
        dataSet.sparkSession().udf().register("middle", new MiddleValue());
        String rankSql = filterSql("origin", "middle", rankColumn, columns);
        dataSet.sparkSession().sql(rankSql).createOrReplaceTempView("a");
        List<String> strings = new ArrayList<>(Arrays.asList(columns));
        strings.add(rankColumn);
        String[] restrictColumns = strings.toArray(new String[strings.size()]);
        Dataset<Row> found = dataSet.sparkSession().sql(joinSql("origin", "a", restrictColumns));
        //在rankColumn和columns的限制下也有可能重复
        return dropDuplicatesRandow(found, restrictColumns);
    }

    /**
     * 去重：保留指定值
     *
     * @param dataSet
     * @param rankColumn
     * @param columns
     * @return
     */
    public Dataset<Row> dropDuplicatesCertain(Dataset<Row> dataSet, String rankColumn, String rankValue, String... columns) {
        dataSet.createOrReplaceTempView("origin");
        dataSet.sparkSession().udf().register("certain", new CertainValue());
        String rankSql = filterSqlCertain("origin", "certain", rankColumn, rankValue.trim(), columns);
        dataSet.sparkSession().sql(rankSql).createOrReplaceTempView("a");
        List<String> strings = new ArrayList<>(Arrays.asList(columns));
        strings.add(rankColumn);
        String[] restrictColumns = strings.toArray(new String[strings.size()]);
        //FIXME
        Dataset<Row> found = dataSet.sparkSession().sql(joinSql("origin", "a", restrictColumns));
        //在rankColumn和columns的限制下也有可能重复
        return dropDuplicatesRandow(found, restrictColumns);
    }

    private String filterSql(String viewName, String funcName, String rankColumn, String... columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("select `").append(Joiner.on("` , `").join(columns));
        sb.append("` , ").append(funcName).append("(`").append(rankColumn).append("`) as `").append(rankColumn).append("` from `").append(viewName).append("` ");
        sb.append("group by `").append(Joiner.on("` , `").join(columns)).append("`");
        return sb.toString();
    }

    private String filterSqlCertain(String viewName, String funcName, String rankColumn, String rankValue, String... columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("select `").append(Joiner.on("` , `").join(columns));
        sb.append("` , ").append(funcName).append("(`").append(rankColumn);
        sb.append("`, '").append(rankValue).append("'")
                .append(") as `").append(rankColumn).append("` from `").append(viewName).append("` ");
        sb.append("group by `").append(Joiner.on("` , `").join(columns)).append("`");
        return sb.toString();
    }

    private String joinSql(String viewName, String filterViewName, String... columns) {
        String view = "`" + viewName + "`";
        String filterView = "`" + filterViewName + "`";
        StringBuilder sb = new StringBuilder();
        sb.append("select ").append(view).append(".* from ").append(filterView).append(" left join ").append(view).append(" on ");
        List<String> conditions = new ArrayList<>();
        for (String column : columns) {
            String conditionStr = view + ".`" + column + "` = " + filterView + ".`" + column + "` ";
            conditions.add(conditionStr);
        }
        sb.append(Joiner.on("and ").join(conditions));
        return sb.toString();
    }

//    public static void main(String[] args) {
////        String sql = new DeduplicateDemo().filterSql("tempView", "max", "c3", "c1", "c2");
////        String sql = new duplicate.DeduplicateDemo().joinSql("origin", "a", "c1", "c2", "c3");
//        String sql = new DeduplicateDemo().filterSqlCertain("temp", "certain", "c5", "ddd", "c1", "c2");
//        System.out.println(sql);
//    }

}
