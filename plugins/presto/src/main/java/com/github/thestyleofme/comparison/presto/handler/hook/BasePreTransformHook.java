package com.github.thestyleofme.comparison.presto.handler.hook;

import java.util.List;

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
    public boolean skip(List<SkipCondition> skipConditionList) {
        List<String> sqlList = generateSqlByCondition(skipConditionList);
        return execSqlAndComputeSkip(sqlList);
    }

    /**
     * 执行sql并计算是否跳过
     *
     * @param sqlList List<String>
     * @return boolean
     */
    protected abstract boolean execSqlAndComputeSkip(List<String> sqlList);

    /**
     * 生成预比对sql集合
     *
     * @param skipConditionList List<SkipCondition>
     * @return List<String>
     */
    protected abstract List<String> generateSqlByCondition(List<SkipCondition> skipConditionList);
}
