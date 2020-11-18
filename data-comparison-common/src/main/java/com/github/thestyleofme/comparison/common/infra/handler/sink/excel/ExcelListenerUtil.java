package com.github.thestyleofme.comparison.common.infra.handler.sink.excel;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/18 17:43
 * @since 1.0.0
 */
public class ExcelListenerUtil {

    private ExcelListenerUtil() {
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> rowToMap(Object object, List<List<String>> excelHeader) {
        Map<Integer, Object> row = (LinkedHashMap<Integer, Object>) object;
        Map<String, Object> map = new LinkedHashMap<>();
        for (Map.Entry<Integer, Object> entry : row.entrySet()) {
            String name = excelHeader.get(entry.getKey()).get(0);
            map.put(name, entry.getValue());
        }
        return map;
    }
}
