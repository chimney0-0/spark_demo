package tag;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author chimney
 */
public class AbnormalAccumulator extends AccumulatorV2<InputData, Map<Integer, Long>> {

	/**
	 * 字段异常值数量 fieldId -> abnormalCount
	 */
	private Map<Integer, Long> fieldAbnormalCount = new HashMap<>();

	@Override
	public boolean isZero() {
		return fieldAbnormalCount.isEmpty();
	}

	@Override
	public AccumulatorV2<InputData, Map<Integer, Long>> copy() {
		AbnormalAccumulator abnormalAccumulator = new AbnormalAccumulator();
		Map<Integer, Long> countMap = new HashMap<>(this.fieldAbnormalCount.size());
		fieldAbnormalCount.forEach(countMap::put);
		abnormalAccumulator.setFieldAbnormalCount(countMap);
		return abnormalAccumulator;
	}

	@Override
	public void reset() {
		this.fieldAbnormalCount = new HashMap<>(16);
	}

	@Override
	public void add(InputData inputData) {
		String value = inputData.getValue();
		Integer fieldId = inputData.getFieldId();
		List<TagS> tagList = inputData.getTagList();
		Long abnormalCount = fieldAbnormalCount.get(fieldId);
		if (abnormalCount == null) {
			abnormalCount = 0L;
		}
		// 计算异常值
		if (tagList != null && !tagList.isEmpty()) {
			for (TagS tag : tagList) {
				String exp = tag.getTagRegExp();
				String type = tag.getTagType();
				boolean isAllowNull = tag.getIsAllowNull();
				if (ValidationRuleTypeEnum.javascript.name().equals(type)) {
					if (value != null) {
						Map<String, Object> map = new HashMap<>(1);
						map.put("value", value);
						Object object = JsFunction.execute(map, exp, OnErrorTypeEnum.SET_TO_BLANK.name());
						boolean result = Boolean.valueOf(object.toString());
						if (!result) {
							abnormalCount++;
							break;
						}
					} else {
						if (!isAllowNull) {
							abnormalCount++;
							break;
						}
					}
				} else if (ValidationRuleTypeEnum.regex.name().equals(type)) {
					if (value != null) {
						if (!Pattern.matches(exp, value)) {
							abnormalCount++;
							break;
						}
					} else {
						if (!isAllowNull) {
							abnormalCount++;
							break;
						}
					}
				} else if (ValidationRuleTypeEnum.innerFunction.name().equals(type)) {
					if (ValidationRuleTypeEnum.identity.name().equals(exp)) {
						if (value != null) {
							if (!InnerFunctionUtils.checkID(value)) {
								abnormalCount++;
								break;
							}
						} else {
							if (!isAllowNull) {
								abnormalCount++;
								break;
							}
						}
					} else if (ValidationRuleTypeEnum.vin.name().equals(exp)) {
						if (value != null) {
							if (!InnerFunctionUtils.checkVIN(value)) {
								abnormalCount++;
								break;
							}
						} else {
							if (!isAllowNull) {
								abnormalCount++;
								break;
							}
						}
					}
				}
			}
		}
		fieldAbnormalCount.put(fieldId, abnormalCount);
	}

	@Override
	public void merge(AccumulatorV2<InputData, Map<Integer, Long>> accumulatorV2) {
		Map<Integer, Long> toMerge = accumulatorV2.value();
		if (toMerge == null || toMerge.isEmpty()) {
			return;
		}
		if (this.fieldAbnormalCount == null || this.fieldAbnormalCount.isEmpty()) {
			this.fieldAbnormalCount = toMerge;
		} else {
			for (Map.Entry<Integer, Long> entry : toMerge.entrySet()) {
				Integer fieldId = entry.getKey();
				Long toAdd = entry.getValue();
				// 这是什么操作?
				this.fieldAbnormalCount.merge(fieldId, toAdd, (a, b) -> a + b);
			}
		}
	}

	@Override
	public Map<Integer, Long> value() {
		return fieldAbnormalCount;
	}

	public Map<Integer, Long> getFieldAbnormalCount() {
		return fieldAbnormalCount;
	}

	public void setFieldAbnormalCount(Map<Integer, Long> fieldAbnormalCount) {
		this.fieldAbnormalCount = fieldAbnormalCount;
	}
}
