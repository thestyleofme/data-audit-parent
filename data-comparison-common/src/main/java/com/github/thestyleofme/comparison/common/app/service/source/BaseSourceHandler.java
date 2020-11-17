package com.github.thestyleofme.comparison.common.app.service.source;

import java.util.Map;

import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;


/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 15:12
 * @since 1.0.0
 */
public interface BaseSourceHandler {

    /**
     * 处理job source
     *
     * @param comparisonJob ComparisonJob
     * @param env           Map
     * @param sourceMap     Map
     * @return SourceDataMapping SourceDataMapping
     */
    SourceDataMapping handle(ComparisonJob comparisonJob,
                             Map<String, Object> env,
                             Map<String, Object> sourceMap);
}
