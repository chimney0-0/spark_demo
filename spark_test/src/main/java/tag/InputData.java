package tag;

import java.io.Serializable;
import java.util.List;

/**
 * @author chimney
 */
public class InputData implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 索引
	 */
	private Integer index;

	private Integer fieldId;

	/**
	 * 列名
	 */
	private String columnName;

	private String value;
	

	private List<TagS> tagList;

	/**
	 * 是否精确计算
	 */
	private boolean accurate;

	public boolean isAccurate() {
		return accurate;
	}

	public void setAccurate(boolean accurate) {
		this.accurate = accurate;
	}

	public String getValue() {
		return value;
	}

	public InputData(Integer index, String columnName, String value) {
		super();
		this.index = index;
		this.columnName = columnName;
		this.value = value;
	}

	public List<TagS> getTagList() {
		return tagList;
	}

	public void setTagList(List<TagS> tagList) {
		this.tagList = tagList;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Integer getIndex() {
		return index;
	}

	public void setIndex(Integer index) {
		this.index = index;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public Integer getFieldId() {
		return fieldId;
	}

	public void setFieldId(Integer fieldId) {
		this.fieldId = fieldId;
	}
}
