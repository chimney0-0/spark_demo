package tag;

import java.io.Serializable;
import java.util.List;

/**
 * @author chimney
 */
public class FieldTagDetail implements Serializable {

	private static final long serialVersionUID = -65351428008397710L;
	private Integer fieldId;

	private String fieldName;

	private List<TagS> tagList;

	public Integer getFieldId() {
		return fieldId;
	}

	public void setFieldId(Integer fieldId) {
		this.fieldId = fieldId;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public List<TagS> getTagList() {
		return tagList;
	}

	public void setTagList(List<TagS> tagList) {
		this.tagList = tagList;
	}
}
