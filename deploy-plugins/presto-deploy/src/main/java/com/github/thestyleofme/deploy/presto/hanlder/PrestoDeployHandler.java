package com.github.thestyleofme.deploy.presto.hanlder;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.baomidou.mybatisplus.core.toolkit.StringPool;
import com.github.thestyleofme.comparison.common.app.service.deploy.BaseDeployHandler;
import com.github.thestyleofme.comparison.common.domain.*;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.DeployType;
import com.github.thestyleofme.comparison.common.infra.constants.CommonConstant;
import com.github.thestyleofme.comparison.common.infra.constants.DeployStrategyEnum;
import com.github.thestyleofme.comparison.common.infra.constants.JobStatusEnum;
import com.github.thestyleofme.comparison.common.infra.constants.PrestoConstant;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.PrestoUtils;
import com.github.thestyleofme.comparison.common.infra.utils.SqlGeneratorUtil;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author siqi.hou@hand-china.com
 * @date 2020-11-19 11:45
 */
@DeployType(CommonConstant.Deploy.PRESTO_DEPLOY)
@Component
@Slf4j
public class PrestoDeployHandler implements BaseDeployHandler {

    private final BaseDeployExecutor replaceDeployExecutor;

    public PrestoDeployHandler(BaseDeployExecutor replaceDeployExecutor) {
        this.replaceDeployExecutor = replaceDeployExecutor;
    }

    @Override
    public void handle(ComparisonJob comparisonJob, DeployInfo deployInfo) {
        String status = comparisonJob.getStatus();
        if (!JobStatusEnum.SUCCESS.name().equalsIgnoreCase(status)) {
            throw new HandlerException("hdsp.xadt.error.deploy.status_not_success", status);
        }
        String strategy = Optional.ofNullable(deployInfo.getStrategy()).orElse(DeployStrategyEnum.REPLACE.name());
        if (DeployStrategyEnum.REPLACE.name().equalsIgnoreCase(strategy)) {
            AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
            // 基于sql 实现数据补偿
            doByPrestoSql(appConf);
            // 基于第三方数据进行数据补偿
            doByOther();
        }
    }

    private void doByPrestoSql(AppConf appConf) {


        for (Map.Entry<String, Map<String, Object>> entry : appConf.getTransform().entrySet()) {
            PrestoInfo prestoInfo = PrestoUtils.getPrestoInfo(appConf, entry.getValue());
            JobEnv jobEnv = BeanUtils.map2Bean(appConf.getEnv(), JobEnv.class);
            String sqls = SqlGeneratorUtil.generateSql(prestoInfo, jobEnv);
            String[] sqlArray = sqls.split(StringPool.SEMICOLON);
            String targetTable = String.format(PrestoConstant.SqlConstant.TABLE_FT, prestoInfo.getTargetPrestoCatalog(),
                    prestoInfo.getTargetSchema(), prestoInfo.getTargetTable());

            // 获取列的映射关系
            List<String> sourceColList = jobEnv.getColMapping().stream()
                    .map(map -> BeanUtils.map2Bean(map, ColMapping.class))
                    .map(ColMapping::getTargetCol)
                    .collect(Collectors.toList());

//            String.format(PrestoConstant.SqlConstant.BASE_INSERT_INTO_SQL, targetTable, , selectSql);
        }
    }

    private void doByOther() {
    }
}
