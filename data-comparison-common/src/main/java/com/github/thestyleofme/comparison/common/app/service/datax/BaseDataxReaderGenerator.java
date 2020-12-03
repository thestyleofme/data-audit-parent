package com.github.thestyleofme.comparison.common.app.service.datax;

import java.util.Map;

import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.domain.entity.Reader;

/**
 * <p>Reader生成接口</p>
 *
 * @author hsq 2020/12/03 14:47
 * @since 1.0.0
 */
public interface BaseDataxReaderGenerator {
    /**
     * 根据不同来源，生成datax同步脚本的Reader
     *
     * @param tenantId      租户id
     * @param comparisonJob job任务
     * @param sinkMap       sink配置信息
     * @param syncType      同步类型
     * @return reader内容
     */
    Reader generate(Long tenantId, ComparisonJob comparisonJob, Map<String, Object> sinkMap, Integer syncType);
}
