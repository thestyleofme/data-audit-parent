package com.github.thestyleofme.comparison.common.app.service.pretransform;

import java.util.List;

import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.entity.DataInfo;
import com.github.thestyleofme.comparison.common.domain.entity.SkipCondition;

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
    public boolean skip(Long tenantId,
                        AppConf appConf,
                        List<SkipCondition> skipConditionList,
                        HandlerResult handlerResult) {
        DataInfo dataInfo = prepareDataInfo(tenantId, appConf);
        List<String> sqlList = generateSqlByCondition(dataInfo, skipConditionList);
        return execSqlAndComputeSkip(tenantId, dataInfo, sqlList, handlerResult);
    }

    /**
     * 封装两个数据源的基本信息，如ComparisonInfo或PrestoInfo
     *
     * @param tenantId 租户id
     * @param appConf  参数配置信息
     * @return 数据源基本信息
     */
    protected abstract DataInfo prepareDataInfo(Long tenantId, AppConf appConf);

    /**
     * 生成预比对sql集合
     *
     * @param dataInfo          两个数据源的基本信息，如ComparisonInfo或PrestoInfo
     * @param skipConditionList List<SkipCondition>
     * @return List<String>
     */
    protected abstract List<String> generateSqlByCondition(DataInfo dataInfo,
                                                           List<SkipCondition> skipConditionList);

    /**
     * 执行sql并计算是否跳过
     *
     * @param tenantId      租户id
     * @param dataInfo      两个数据源的基本信息，如ComparisonInfo或PrestoInfo
     * @param sqlList       List<String>
     * @param handlerResult HandlerResult
     * @return boolean
     */
    protected abstract boolean execSqlAndComputeSkip(Long tenantId,
                                                     DataInfo dataInfo,
                                                     List<String> sqlList,
                                                     HandlerResult handlerResult);

}
