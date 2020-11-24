package com.github.thestyleofme.comparison.common.app.service.deploy;

import com.github.thestyleofme.comparison.common.domain.DeployInfo;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;

/**
 * <p>
 * description
 * </p>
 *
 * @author hsq 2020/11/20 9:42
 * @since 1.0.0
 */
public interface BaseDeployHandler {

    /**
     * 处理数据补偿任务
     *
     * @param comparisonJob 数据稽核执行信息
     * @param deployInfo    数据补偿信息
     */
    void handle(ComparisonJob comparisonJob, DeployInfo deployInfo);
}
