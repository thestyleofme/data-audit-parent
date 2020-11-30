package com.github.thestyleofme.comparison.presto.handler.hook;

import java.util.List;

import com.github.thestyleofme.comparison.presto.handler.pojo.SkipCondition;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/30 17:30
 * @since 1.0.0
 */
public interface PreTransformHook {

    /**
     * 返回预比对的处理引擎
     *
     * @return String
     */
    String getName();

    /**
     * 是否跳过具体的稽核
     *
     * @param skipConditionList List<SkipCondition>
     * @return true/false
     */
    boolean skip(List<SkipCondition> skipConditionList);
}
