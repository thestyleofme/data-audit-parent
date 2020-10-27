package com.github.thestyleofme.data.comparison.infra.handler.output;

import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;
import com.github.thestyleofme.data.comparison.infra.handler.HandlerResult;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/27 9:31
 * @since 1.0.0
 */
public interface BaseOutputHandler {

    /**
     * 处理job
     *
     * @param comparisonJob ComparisonJob
     * @param handlerResult HandlerResult
     */
    void handle(ComparisonJob comparisonJob, HandlerResult handlerResult);
}
