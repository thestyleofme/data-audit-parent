package com.github.thestyleofme.comparison.common.app.service.transform;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.thestyleofme.comparison.common.domain.ResultStatistics;
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
    private List<Map<String, Object>> sourceUniqueDataList = new CopyOnWriteArrayList<>();
    /**
     * 目标端有但源端无
     */
    private List<Map<String, Object>> targetUniqueDataList = new CopyOnWriteArrayList<>();

    /**
     * 源端和目标端数据不一样，但主键或唯一性索引一样
     */
    private List<Map<String, Object>> pkOrIndexSameDataList = new CopyOnWriteArrayList<>();

    private ResultStatistics resultStatistics = new ResultStatistics();
}
