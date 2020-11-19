package com.github.thestyleofme.comparison.common.infra.handler.sink.excel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.github.thestyleofme.driver.core.infra.meta.Tuple;

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
    public static List<Tuple<String, String>> rowToTupleList(Object object, List<List<String>> excelHeader) {
        Map<Integer, String> row = (LinkedHashMap<Integer, String>) object;
        List<Tuple<String, String>> tupleList = new ArrayList<>();
        Tuple<String, String> tuple;
        for (Map.Entry<Integer, String> entry : row.entrySet()) {
            String name = excelHeader.get(entry.getKey()).get(0);
            tuple = new Tuple<>(name, entry.getValue());
            tupleList.add(tuple);
        }
        return tupleList;
    }
}
