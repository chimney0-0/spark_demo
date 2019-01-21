package tag;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import javax.swing.text.html.HTML;
import java.util.*;

public class TagTest {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().master("local[4]").appName("Simple Application").getOrCreate();

		AbnormalAccumulator accumulator = new AbnormalAccumulator();
		spark.sparkContext().register(accumulator, "abnormalAccumulator");

		TagS tagS1= new TagS();
		tagS1.setIsAllowNull(true);
		tagS1.setTagType(ValidationRuleTypeEnum.regex.name());
		tagS1.setTagRegExp("\\d+");

		TagS tagS2 = new TagS();
		tagS2.setIsAllowNull(true);
		tagS2.setTagType(ValidationRuleTypeEnum.regex.name());
		tagS2.setTagRegExp("企业");

		List<TagS> tagSList = Arrays.asList(tagS1, tagS2);

		FieldTagDetail field0 = new FieldTagDetail();
		field0.setFieldId(1);
		field0.setFieldName("idcard");
		FieldTagDetail field1 = new FieldTagDetail();
		field1.setFieldId(2);
		field1.setFieldName("tel");
		field1.setTagList(tagSList);
		List<FieldTagDetail> fieldTagList = Arrays.asList(field0, field1);

		Dataset<Row> dataset = spark.read().option("header", true).csv("D:\\data_test\\ceshi2.csv");
		dataset.show();
		System.out.println(dataset.count());

		Map<Integer, FieldTagDetail> fieldMap = new HashMap<>(fieldTagList.size());
		StructField[] structFields = dataset.schema().fields();
		for (int index = 0; index < structFields.length; index++) {
			for (FieldTagDetail fieldValidation : fieldTagList) {
				if (structFields[index].name().equals(fieldValidation.getFieldName())) {
					fieldMap.put(index, fieldValidation);
					break;
				}
			}
		}

		JavaRDD<Row> rdd = dataset.toJavaRDD();
		Long count = rdd.map(f -> {
			for (int index = 0; index < f.size(); index++) {
				String str = f.get(index) == null ? null : f.get(index).toString();
				InputData inputData = new InputData(index, structFields[index].name(), str);
				inputData.setFieldId(fieldMap.get(index).getFieldId());
				inputData.setTagList(fieldMap.get(index).getTagList());
				accumulator.add(inputData);
			}
			return f;
		}).count();

		Map<Integer,Long> result = accumulator.value();
		System.out.println(result);
	}


}
