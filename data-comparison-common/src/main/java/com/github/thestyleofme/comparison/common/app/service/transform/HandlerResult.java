package com.github.thestyleofme.comparison.common.app.service.transform;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 17:07
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HandlerResult {

    /**
     * 源端有但目标端无
     */
    List<LinkedHashMap<String, Object>> sourceUniqueDataList = new ArrayList<>();
    /**
     * 目标端有但源端无
     */
    List<LinkedHashMap<String, Object>> targetUniqueDataList = new ArrayList<>();
    /**
     * 源端和目标端数据不一样，但主键或唯一性索引一样
     */
    List<LinkedHashMap<String, Object>> pkOrIndexSameDataList = new ArrayList<>();
    /**
     * 源端和目标端都有的数据
     * bloom filter 只能说可能存在 不能百分百保证
     */
    List<LinkedHashMap<String, Object>> sameDataList = new ArrayList<>();
}
