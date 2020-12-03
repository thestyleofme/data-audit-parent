package com.github.thestyleofme.comparison.presto.handler.hook;

import java.util.List;

import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.presto.handler.pojo.PrestoInfo;
import com.github.thestyleofme.comparison.presto.handler.pojo.SkipCondition;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/30 17:43
 * @since 1.0.0
 */
public abstract class BasePreTransformHook implements PreTransformHook {

    @Override
    public boolean skip(Long tenantId, PrestoInfo prestoInfo,
                        List<SkipCondition> skipConditionList,
                        HandlerResult handlerResult) {
        List<String> sqlList = generateSqlByCondition(prestoInfo, skipConditionList);
        return execSqlAndComputeSkip(tenantId, prestoInfo, sqlList, handlerResult);
    }

    /**
     * 执行sql并计算是否跳过
     *
     * @param tenantId      租户id
     * @param prestoInfo    PrestoInfo
     * @param sqlList       List<String>
     * @param handlerResult HandlerResult
     * @return boolean
     */
    protected abstract boolean execSqlAndComputeSkip(Long tenantId, PrestoInfo prestoInfo, List<String> sqlList, HandlerResult handlerResult);

    /**
     * 生成预比对sql集合
     *
     * @param prestoInfo        PrestoInfo
     * @param skipConditionList List<SkipCondition>
     * @return List<String>
     */
    protected abstract List<String> generateSqlByCondition(PrestoInfo prestoInfo, List<SkipCondition> skipConditionList);
}
