package com.github.thestyleofme.data.comparison.infra.handler.transform.java;

import java.util.Map;
import java.util.Optional;

import com.github.thestyleofme.comparison.common.infra.constants.CommonConstant;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/03 11:49
 * @since 1.0.0
 */
public class SelectorUtil {

    private SelectorUtil() {
    }

    public static String getKey(String key, boolean flag) {
        if (key.contains(CommonConstant.ARROW_CONCAT)) {
            if (flag) {
                return key.split(CommonConstant.ARROW_CONCAT)[0];
            } else {
                return key.split(CommonConstant.ARROW_CONCAT)[1];
            }
        } else {
            return key;
        }
    }

    /**
     * map2的key有映射关系
     * 比较两个map相同key的value是否相等
     * 注意，map1的size大于map2
     *
     * @return true/false true代表相等
     */
    public static boolean mapCompare(Map<String, Object> map1, Map<String, Object> map2) {
        boolean isEqual = true;
        for (Map.Entry<String, Object> entry2 : map2.entrySet()) {
            String m2value = String.valueOf(entry2.getValue());
            Optional<Map.Entry<String, Object>> optional = map1.entrySet().stream()
                    .filter(o -> entry2.getKey().contains(o.getKey()))
                    .findFirst();
            if(optional.isPresent()){
                String m1value = String.valueOf(map1.get(optional.get().getKey()));
                if (!m1value.equals(m2value)) {
                    isEqual = false;
                    break;
                }
            }
        }
        return isEqual;
    }
}
