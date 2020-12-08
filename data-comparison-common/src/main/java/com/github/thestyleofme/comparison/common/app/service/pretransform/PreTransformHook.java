package com.github.thestyleofme.comparison.common.app.service.pretransform;


import java.util.List;

import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.entity.SkipCondition;

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
     * @param tenantId          租户id
     * @param appConf           参数配置信息
     * @param skipConditionList List<SkipCondition>
     * @param handlerResult     存储预比对结果
     * @return true/false
     */
    boolean skip(Long tenantId,
                 AppConf appConf,
                 List<SkipCondition> skipConditionList,
                 HandlerResult handlerResult);
}
