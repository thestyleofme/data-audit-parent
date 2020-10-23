package com.github.thestyleofme.data.comparison.infra.handler.comparison;

import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 15:12
 * @since 1.0.0
 */
public interface BaseComparisonHandler {

    /**
     * 处理job
     *
     * @param comparisonJob ComparisonJob
     * @return ComparisonMapping ComparisonMapping
     */
    ComparisonMapping handle(ComparisonJob comparisonJob);
}
