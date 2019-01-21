package tag;

/**
 * 脚本执行异常时操作处理类型
 * @author zhangqianfeng
 *
 */
public enum OnErrorTypeEnum {
	/**
	 * 保持原数据
	 */
	KEEP_ORIGINAL  , 
	/**
	 * 设置为空
	 */
	SET_TO_BLANK,
	/**
	 * 保存错误信息
	 */
	STORE_ERROR
	
}
