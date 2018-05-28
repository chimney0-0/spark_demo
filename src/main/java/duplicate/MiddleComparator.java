package duplicate;

import java.math.BigDecimal;
import java.util.Comparator;

/**
 * 自定义排序规则：数字按照大小；字符串按照默认；数字<字符串
 */
class MiddleComparator implements Comparator<String>{
    @Override
    public int compare(String o1, String o2) {
        boolean flag = isNumeric(o2);
        //o1为数值
        if (isNumeric(o1)) {
            if (flag) return new BigDecimal(o1).compareTo(new BigDecimal(o2));
            else return -1;
        }
        //o1不为数值
        else {
            if (flag) return 1;
            else return o1.compareTo(o2);
        }
    }

    private boolean isNumeric(String str) {
        try {
            new BigDecimal(str);
        } catch (Exception e) {
            return false;//异常 说明包含非数字。
        }
        return true;
    }
}
