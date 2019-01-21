package tag;

import java.io.Serializable;

/**
 * @author chimney
 */
public class TagS implements Serializable{

	private static final long serialVersionUID = -3352771842142428512L;
	private Integer tagId;

	private String tagName;

	private String tagRegExp;

	private String tagDesc;

	private Boolean isPredefined;

	private String tagType;

	private Double thresholdValue;

	private Boolean isAllowNull;

	public TagS() {
	}

	public TagS(Integer tagId, String tagName, String tagRegExp, String tagDesc, Boolean isPredefined, String tagType, Double thresholdValue, Boolean isAllowNull) {
		this.tagId = tagId;
		this.tagName = tagName;
		this.tagRegExp = tagRegExp;
		this.tagDesc = tagDesc;
		this.isPredefined = isPredefined;
		this.tagType = tagType;
		this.thresholdValue = thresholdValue;
		this.isAllowNull = isAllowNull;
	}

	public Integer getTagId() {
		return tagId;
	}

	public void setTagId(Integer tagId) {
		this.tagId = tagId;
	}

	public String getTagName() {
		return tagName;
	}

	public void setTagName(String tagName) {
		this.tagName = tagName;
	}

	public String getTagRegExp() {
		return tagRegExp;
	}

	public void setTagRegExp(String tagRegExp) {
		this.tagRegExp = tagRegExp;
	}

	public String getTagDesc() {
		return tagDesc;
	}

	public void setTagDesc(String tagDesc) {
		this.tagDesc = tagDesc;
	}

	public Boolean getPredefined() {
		return isPredefined;
	}

	public void setPredefined(Boolean predefined) {
		isPredefined = predefined;
	}

	public String getTagType() {
		return tagType;
	}

	public void setTagType(String tagType) {
		this.tagType = tagType;
	}

	public Double getThresholdValue() {
		return thresholdValue;
	}

	public void setThresholdValue(Double thresholdValue) {
		this.thresholdValue = thresholdValue;
	}

	public Boolean getIsAllowNull() {
		return isAllowNull;
	}

	public void setIsAllowNull(Boolean isAllowNull) {
		this.isAllowNull = isAllowNull;
	}
}
