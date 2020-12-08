package com.github.thestyleofme.comparison.common.app.service.transform;

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
public interface BaseTransformHandler {

    /**
     * 处理job
     *
     * @param comparisonJob ComparisonJob
     * @param env           Map
     * @param transformMap  Map
     * @param handlerResult HandlerResult
     */
    void handle(ComparisonJob comparisonJob,
                Map<String, Object> env,
                Map<String, Object> transformMap,
                HandlerResult handlerResult);
}
