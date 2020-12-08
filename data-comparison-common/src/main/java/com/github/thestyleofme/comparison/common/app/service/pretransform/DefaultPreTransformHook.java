package com.github.thestyleofme.comparison.common.app.service.pretransform;

import java.util.List;

import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.ComparisonInfo;
import com.github.thestyleofme.comparison.common.domain.entity.DataInfo;
import com.github.thestyleofme.comparison.common.domain.entity.SkipCondition;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import org.springframework.stereotype.Component;

/**
 * <p>通过数据源进行预处理</p>
 *
 * @author hsq 2020/12/04 15:44
 * @since 1.0.0
 */
@Component
public class DefaultPreTransformHook extends BasePreTransformHook {

    private final DriverSessionService driverSessionService;

    public DefaultPreTransformHook(DriverSessionService driverSessionService) {
        this.driverSessionService = driverSessionService;
    }

    @Override
    public String getName() {
        return "default";
    }

    @Override
    protected DataInfo prepareDataInfo(Long tenantId, AppConf appConf) {
        return CommonUtil.getComparisonInfo(appConf.getEnv());
    }

    @Override
    protected List<String> generateSqlByCondition(DataInfo dataInfo, List<SkipCondition> skipConditionList) {
        return null;
    }


    @Override
    protected boolean execSqlAndComputeSkip(Long tenantId, DataInfo dataInfo,
                                            List<String> sqlList,
                                            HandlerResult handlerResult) {
        ComparisonInfo comparisonInfo = (ComparisonInfo) dataInfo;
        // todo 预处理
        return true;
    }

}
