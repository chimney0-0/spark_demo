package tag;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 内置函数计算类
 *
 * @author chimney
 */
public class InnerFunctionUtils {

	/**
	 * 身份证前17位每位加权因子
	 */
	private static int[] power = {7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2};

	/**
	 * 身份证第18位校检码
	 */
	private static String[] refNumber = {"1", "0", "X", "9", "8", "7", "6", "5", "4", "3", "2"};

	/**
	 * 省(直辖市)码表
	 */
	private static String[] provinceCode = {"11", "12", "13", "14", "15", "21", "22",
			"23", "31", "32", "33", "34", "35", "36", "37", "41", "42", "43",
			"44", "45", "46", "50", "51", "52", "53", "54", "61", "62", "63",
			"64", "65", "71", "81", "82", "91"};

	/**
	 * 检验VIN格式
	 *
	 * @param vin
	 * @return
	 */
	public static boolean checkVIN(String vin) {
		if (vin == null) {
			return false;
		}
		String upperVin = vin.toUpperCase();

		String exclude1 = "O";
		String exclude2 = "I";
		char std = 'X';

		//排除字母O、I
		if (upperVin.contains(exclude1) || upperVin.contains(exclude2)) {
			return false;
		}
		Integer stdLength = 17;
		Integer divisor = 11;
		Integer verifyIndex = 8;
		// 长度不为17
		if (vin.length() != stdLength) {
			return false;
		}

		// 含有数字字母外的其他字符
		String basicRegex = "[0-9a-zA-z]+";
		if (!vin.matches(basicRegex)) {
			return false;
		}

		Map<Integer, Integer> vinMapWeighting;
		Map<Character, Integer> vinMapValue;
		vinMapWeighting = new HashMap<>(32);
		vinMapValue = new HashMap<>(48);

		vinMapWeighting.put(1, 8);
		vinMapWeighting.put(2, 7);
		vinMapWeighting.put(3, 6);
		vinMapWeighting.put(4, 5);
		vinMapWeighting.put(5, 4);
		vinMapWeighting.put(6, 3);
		vinMapWeighting.put(7, 2);
		vinMapWeighting.put(8, 10);
		vinMapWeighting.put(9, 0);
		vinMapWeighting.put(10, 9);
		vinMapWeighting.put(11, 8);
		vinMapWeighting.put(12, 7);
		vinMapWeighting.put(13, 6);
		vinMapWeighting.put(14, 5);
		vinMapWeighting.put(15, 4);
		vinMapWeighting.put(16, 3);
		vinMapWeighting.put(17, 2);

		vinMapValue.put('0', 0);
		vinMapValue.put('1', 1);
		vinMapValue.put('2', 2);
		vinMapValue.put('3', 3);
		vinMapValue.put('4', 4);
		vinMapValue.put('5', 5);
		vinMapValue.put('6', 6);
		vinMapValue.put('7', 7);
		vinMapValue.put('8', 8);
		vinMapValue.put('9', 9);
		vinMapValue.put('A', 1);
		vinMapValue.put('B', 2);
		vinMapValue.put('C', 3);
		vinMapValue.put('D', 4);
		vinMapValue.put('E', 5);
		vinMapValue.put('F', 6);
		vinMapValue.put('G', 7);
		vinMapValue.put('H', 8);
		vinMapValue.put('J', 1);
		vinMapValue.put('K', 2);
		vinMapValue.put('M', 4);
		vinMapValue.put('L', 3);
		vinMapValue.put('N', 5);
		vinMapValue.put('P', 7);
		vinMapValue.put('R', 9);
		vinMapValue.put('S', 2);
		vinMapValue.put('T', 3);
		vinMapValue.put('U', 4);
		vinMapValue.put('V', 5);
		vinMapValue.put('W', 6);
		vinMapValue.put('X', 7);
		vinMapValue.put('Y', 8);
		vinMapValue.put('Z', 9);

		char[] vinArr = upperVin.toCharArray();
		int amount = 0;
		for (int i = 0; i < vinArr.length; i++) {
			//VIN码从从第一位开始，码数字的对应值×该位的加权值，计算全部17位的乘积值相加
			amount += vinMapValue.get(vinArr[i]) * vinMapWeighting.get(i + 1);
		}
		//乘积值相加除以11、若余数为10，即为字母X
		if (amount % divisor == divisor - 1) {
			return vinArr[verifyIndex] == std;
		} else {
			//VIN码从从第一位开始，码数字的对应值×该位的加权值，
			// 计算全部17位的乘积值相加除以11，所得的余数，即为第九位校验值
			return amount % divisor == vinMapValue.get(vinArr[verifyIndex]);
		}
	}

	/**
	 * 二代身份证号码有效性校验
	 *
	 * @param idNo
	 * @return
	 */
	public static boolean checkID(String idNo) {
		return isIdNoPattern(idNo) && isValidProvinceId(idNo.substring(0, 2))
				&& isValidDate(idNo.substring(6, 14)) && checkIdNoLastNum(idNo);
	}

	/**
	 * 二代身份证正则表达式
	 *
	 * @param idNo
	 * @return
	 */
	private static boolean isIdNoPattern(String idNo) {
		return Pattern.matches("^[1-9]\\d{5}[1-9]\\d{3}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}([\\d|x|X]{1})$", idNo);
	}

	/**
	 * 检查身份证的省份信息是否正确
	 *
	 * @param provinceId
	 * @return
	 */
	private static boolean isValidProvinceId(String provinceId) {
		for (String id : provinceCode) {
			if (id.equals(provinceId)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 判断日期是否有效
	 *
	 * @param inDate
	 * @return
	 */
	private static boolean isValidDate(String inDate) {
		if (inDate == null) {
			return false;
		}
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		if (inDate.trim().length() != dateFormat.toPattern().length()) {
			return false;
		}
		//执行严格的日期匹配
		dateFormat.setLenient(false);
		try {
			dateFormat.parse(inDate.trim());
		} catch (ParseException e) {
			return false;
		}
		return true;
	}

	/**
	 * 计算身份证的第十八位校验码
	 *
	 * @param cardIdArray
	 * @return
	 */
	private static String sumPower(int[] cardIdArray) {
		int result = 0;
		for (int i = 0; i < power.length; i++) {
			result += power[i] * cardIdArray[i];
		}
		return refNumber[(result % 11)];
	}

	/**
	 * 校验身份证第18位是否正确(只适合18位身份证)
	 *
	 * @param idNo
	 * @return
	 */
	private static boolean checkIdNoLastNum(String idNo) {
		Integer stdLength = 18;
		if (idNo.length() != stdLength) {
			return false;
		}
		char[] tmp = idNo.toCharArray();
		int[] cardidArray = new int[tmp.length - 1];
		int i;
		for (i = 0; i < tmp.length - 1; i++) {
			cardidArray[i] = Integer.parseInt(tmp[i] + "");
		}
		String checkCode = sumPower(cardidArray);
		String lastNum = tmp[tmp.length - 1] + "";
		String lower = "x";
		if (lower.equals(lastNum)) {
			lastNum = lastNum.toUpperCase();
		}
		return checkCode.equals(lastNum);
	}

	public static void main(String[] args) {
		System.out.println(checkVIN("我是一个粉刷匠"));
	}

}
