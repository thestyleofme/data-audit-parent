package com.github.thestyleofme.data.comparison.infra.handler.transform.java;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.infra.constants.RowTypeEnum;
import lombok.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/02 11:16
 * @since 1.0.0
 */
public class DataSelector {

    private DataSelector() {
    }

    private List<Map<String, Object>> mainList;
    private List<Map<String, Object>> subList;
    private List<String> paramNames;

    public static DataSelector init(List<String> paramNames) {
        DataSelector dataSelector = new DataSelector();
        dataSelector.paramNames = paramNames;
        return dataSelector;
    }

    public DataSelector addMain(List<Map<String, Object>> mainList) {
        this.mainList = mainList;
        return this;
    }

    public DataSelector addSub(List<Map<String, Object>> subList) {
        this.subList = subList;
        return this;
    }

    public Result select() {
        Result result = new Result();
        // A有B无 AB某些字段数据不一致
        select(result, this.subList, this.mainList, true);
        // A无B有
        select(result, this.mainList, this.subList, false);
        return result;
    }

    private void select(Result result, List<Map<String, Object>> list1,
                        List<Map<String, Object>> list2,
                        boolean flag) {
        SelectCollectorImpl.Result mainResult = list1.stream().collect(new SelectCollectorImpl(this.paramNames));
        Set<String> valueSet = mainResult.getValueSet();
        for (Map<String, Object> map : list2) {
            SelectCollectorImpl.Result subResult = SelectCollectorImpl.Result.init(map, this.paramNames);
            if (!valueSet.contains(subResult.getFirstValue())) {
                Map<String, Set<Object>> valueMap = mainResult.getValueMap();
                List<String> paramNameList = valueMap.entrySet()
                        .stream()
                        .filter(entry -> !entry.getValue().contains(map.get(entry.getKey())))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
                if (flag) {
                    doMain(result, map, paramNameList);
                } else {
                    doSub(result, map, paramNameList);
                }
            }
        }
    }

    private void doSub(Result result, Map<String, Object> map, List<String> paramNameList) {
        if (paramNameList.size() == this.paramNames.size()) {
            // A无B有
            result.addDiff(map, paramNameList, RowTypeEnum.DELETED.name());
        }
    }

    private void doMain(Result result, Map<String, Object> map, List<String> paramNameList) {
        if (paramNameList.size() == this.paramNames.size()) {
            // A有B无
            result.addDiff(map, paramNameList, RowTypeEnum.INSERT.name());
        } else {
            // AB某些字段数据不一致
            result.addDiff(map, paramNameList, RowTypeEnum.DIFFERENT.name());
        }
    }

    @Getter
    @Setter(value = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @ToString
    public static class Result {
        private List<Diff> diffList = new ArrayList<>();

        public void addDiff(Map<String, Object> data, List<String> paramNames, String type) {
            Diff diff = new Diff(data, paramNames, type);
            this.diffList.add(diff);
        }

        @Getter
        @Setter(value = AccessLevel.PRIVATE)
        @AllArgsConstructor(access = AccessLevel.PRIVATE)
        @ToString
        static class Diff {
            private Map<String, Object> data;
            private List<String> paramNames;
            private String type;
        }
    }
}
