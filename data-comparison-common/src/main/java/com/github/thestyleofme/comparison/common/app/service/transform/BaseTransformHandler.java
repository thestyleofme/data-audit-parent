package com.github.thestyleofme.comparison.common.app.service.transform;

import java.util.Map;

import com.github.thestyleofme.comparison.common.app.service.source.SourceDataMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;


/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 15:12
 * @since 1.0.0
 */
public interface BaseTransformHandler {

    /**
     * 处理job
     *
     * @param comparisonJob     ComparisonJob
     * @param env               Map
     * @param sourceDataMapping ComparisonMapping
     * @return HandlerResult
     */
    HandlerResult handle(ComparisonJob comparisonJob,
                         Map<String, Object> env,
                         SourceDataMapping sourceDataMapping);
}
