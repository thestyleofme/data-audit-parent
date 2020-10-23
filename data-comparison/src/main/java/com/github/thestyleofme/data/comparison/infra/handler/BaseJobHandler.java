package com.github.thestyleofme.data.comparison.infra.handler;

import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 15:12
 * @since 1.0.0
 */
public interface BaseJobHandler {

    /**
     * 处理job
     *
     * @param comparisonJob ComparisonJob
     * @return HandlerResult
     */
    HandlerResult handle(ComparisonJob comparisonJob);
}
