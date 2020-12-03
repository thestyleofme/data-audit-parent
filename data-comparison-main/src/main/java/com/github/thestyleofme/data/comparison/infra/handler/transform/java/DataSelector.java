package com.github.thestyleofme.data.comparison.infra.handler.transform.java;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.infra.constants.RowTypeEnum;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import lombok.*;
import org.springframework.util.CollectionUtils;

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

    public Result select(List<Map<String, Object>> indexMapping) {
        Result result = new Result();
        // A有B无 AB某些字段数据不一致
        select(result, this.subList, this.mainList, indexMapping, true);
        // A无B有
        select(result, this.mainList, this.subList, indexMapping, false);
        return result;
    }

    private void select(Result result, List<Map<String, Object>> list1,
                        List<Map<String, Object>> list2,
                        List<Map<String, Object>> indexMapping,
                        boolean flag) {
        List<String> indexList = getIndexList(indexMapping, flag);
        SelectCollectorImpl.Result mainResult = list1.stream().collect(new SelectCollectorImpl(this.paramNames, indexList, !flag));
        Set<String> valueSet = mainResult.getValueSet();
        for (Map<String, Object> map : list2) {
            SelectCollectorImpl.Result subResult = SelectCollectorImpl.Result.init(map, this.paramNames, indexList, flag);
            if (!valueSet.contains(subResult.getFirstValue())) {
                Map<String, Set<Object>> valueMap = mainResult.getValueMap();
                List<Map<String, Object>> indexValueList = mainResult.getIndexValueList();
                List<String> paramNameList = genParamNameList(flag, map, valueMap);
                if (flag) {
                    doMain(result, map, paramNameList, indexValueList);
                } else {
                    doSub(result, map, paramNameList, indexValueList);
                }
            }
        }
    }

    private List<String> getIndexList(List<Map<String, Object>> indexMapping, boolean flag) {
        List<String> indexList;
        if (flag) {
            indexList = indexMapping.stream()
                    .map(map -> {
                        ColMapping colMapping = BeanUtils.map2Bean(map, ColMapping.class);
                        return colMapping.getSourceCol();
                    })
                    .collect(Collectors.toList());
        } else {
            indexList = indexMapping.stream()
                    .map(map -> {
                        ColMapping colMapping = BeanUtils.map2Bean(map, ColMapping.class);
                        return colMapping.getTargetCol();
                    })
                    .collect(Collectors.toList());
        }
        return indexList;
    }

    private List<String> genParamNameList(boolean flag, Map<String, Object> map, Map<String, Set<Object>> valueMap) {
        return valueMap.entrySet()
                .stream()
                .filter(entry -> {
                    String key = SelectorUtil.getKey(entry.getKey(), flag);
                    Object value = map.get(key);
                    return !entry.getValue().contains(String.valueOf(value));
                })
                .map(entry -> SelectorUtil.getKey(entry.getKey(), flag))
                .collect(Collectors.toList());
    }

    private void doSub(Result result, Map<String, Object> map, List<String> paramNameList, List<Map<String, Object>> indexValueList) {
        if (CollectionUtils.isEmpty(indexValueList)) {
            if (paramNameList.size() == this.paramNames.size()) {
                // A无B有
                result.addDiff(map, paramNameList, RowTypeEnum.DELETED.name());
            }
        } else {
            boolean anyMatch = indexValueList.stream()
                    .anyMatch(map2 -> SelectorUtil.mapCompare(map, map2));
            if (!anyMatch) {
                // A无B有
                result.addDiff(map, paramNameList, RowTypeEnum.DELETED.name());
            }
        }
    }

    private void doMain(Result result, Map<String, Object> map, List<String> paramNameList, List<Map<String, Object>> indexValueList) {
        if (CollectionUtils.isEmpty(indexValueList)) {
            if (paramNameList.size() == this.paramNames.size()) {
                // A有B无
                result.addDiff(map, paramNameList, RowTypeEnum.INSERT.name());
            } else {
                // AB某些字段数据不一致
                result.addDiff(map, paramNameList, RowTypeEnum.DIFFERENT.name());
            }
        } else {
            boolean anyMatch = indexValueList.stream()
                    .anyMatch(map2 -> SelectorUtil.mapCompare(map, map2));
            if (anyMatch) {
                // 主键或唯一索引相同 但是其他字段不同
                result.addDiff(map, paramNameList, RowTypeEnum.UPDATED.name());
            } else {
                // A有B无
                result.addDiff(map, paramNameList, RowTypeEnum.INSERT.name());
            }
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
