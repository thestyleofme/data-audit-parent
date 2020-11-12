package com.github.thestyleofme.comparison.common.app.service.sink;

import java.util.Map;

import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.ComparisonJob;


/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/27 9:31
 * @since 1.0.0
 */
public interface BaseSinkHandler {

    /**
     * 处理job
     * @param comparisonJob ComparisonJob
     * @param env Map
     * @param sinkMap Map
     * @param handlerResult HandlerResult
     */
    void handle(ComparisonJob comparisonJob,
                Map<String, Object> env,
                Map<String, Object> sinkMap,
                HandlerResult handlerResult);
}
