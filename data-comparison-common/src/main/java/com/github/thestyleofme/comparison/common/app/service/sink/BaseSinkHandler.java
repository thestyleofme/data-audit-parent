package com.github.thestyleofme.comparison.common.app.service.sink;

import java.util.Map;

import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.domain.entity.Reader;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;


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
     *
     * @param comparisonJob ComparisonJob
     * @param env           Map
     * @param sinkMap       Map
     * @param handlerResult HandlerResult
     */
    void handle(ComparisonJob comparisonJob,
                Map<String, Object> env,
                Map<String, Object> sinkMap,
                HandlerResult handlerResult);

    /**
     * 对BaseSinkHandler创建代理
     *
     * @return BaseSinkHandler
     */
    default BaseSinkHandler proxy() {
        return this;
    }

    /**
     * 根据不同来源，生成datax同步脚本的Reader
     *
     * @param comparisonJob  job
     * @param sinkMap  sink配置信息
     * @param syncType 同步类型
     * @return reader内容
     */
    default Reader dataxReader(ComparisonJob comparisonJob, Map<String, Object> sinkMap, Integer syncType) {
        throw new HandlerException("hdsp.xadt.err.not.support.datax");
    }
}
