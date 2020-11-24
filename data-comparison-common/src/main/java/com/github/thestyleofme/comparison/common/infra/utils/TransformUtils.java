package com.github.thestyleofme.comparison.common.infra.utils;

import java.util.*;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import org.springframework.util.CollectionUtils;

/**
 * @author hsq
 * @date 2020-11-17 11:48
 */
public class TransformUtils {

    private TransformUtils() {
    }

    /**
     * 按mapping映射顺序对 数据集list进行排序
     *
     * @param jobEnv    JobEnv
     * @param list     数据集合
     * @param position ColMapping.SOURCE/TARGET
     * @return 有序数据集
     */
    public static List<LinkedHashMap<String, Object>> sortListMap(JobEnv jobEnv,
                                                            List<Map<String, Object>> list,
                                                            String position) {
        List<Map<String, Object>> colMapping = jobEnv.getColMapping();
        List<LinkedHashMap<String, Object>> result = new ArrayList<>(list.size());
        if (CollectionUtils.isEmpty(colMapping)) {
            SortedMap<String, Object> sortedMap;
            LinkedHashMap<String, Object> linkedHashMap;
            for (Map<String, Object> map : list) {
                sortedMap = new TreeMap<>(Comparator.reverseOrder());
                sortedMap.putAll(map);
                linkedHashMap = new LinkedHashMap<>(sortedMap);
                result.add(linkedHashMap);
            }
            return result;
        }
        // 根据ColMapping的index进行排序
        List<ColMapping> colMappingList = colMapping.stream()
                .map(map -> BeanUtils.map2Bean(map, ColMapping.class))
                .collect(Collectors.toList());
        LinkedHashMap<String, Object> linkedHashMap = colMappingList.stream()
                .sorted(Comparator.comparingInt(ColMapping::getIndex))
                .collect(Collectors.toMap(
                        o -> {
                            if (ColMapping.SOURCE.equals(position)) {
                                return o.getSourceCol();
                            }
                            return o.getTargetCol();
                        },
                        ColMapping::getIndex,
                        (oldValue, newValue) -> oldValue,
                        LinkedHashMap::new));
        LinkedHashMap<String, Object> temp;
        for (Map<String, Object> map : list) {
            linkedHashMap.putAll(map);
            temp = new LinkedHashMap<>(linkedHashMap);
            result.add(temp);
        }
        return result;
    }
}
