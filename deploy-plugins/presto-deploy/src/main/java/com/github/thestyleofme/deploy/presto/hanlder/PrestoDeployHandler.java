package com.github.thestyleofme.deploy.presto.hanlder;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.app.service.deploy.BaseDeployHandler;
import com.github.thestyleofme.comparison.common.domain.*;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.DeployType;
import com.github.thestyleofme.comparison.common.infra.constants.CommonConstant;
import com.github.thestyleofme.comparison.common.infra.constants.DeployStrategyEnum;
import com.github.thestyleofme.comparison.common.infra.constants.PrestoConstant;
import com.github.thestyleofme.comparison.common.infra.utils.PrestoUtils;
import com.github.thestyleofme.comparison.common.infra.utils.SqlGeneratorUtil;
import com.github.thestyleofme.plugin.core.infra.constants.BaseConstant;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author siqi.hou 2020/11/20 9:43
 * @since 1.0.0
 */
@DeployType(CommonConstant.Deploy.PRESTO)
@Component
@Slf4j
public class PrestoDeployHandler implements BaseDeployHandler {

    private final BaseDeployExecutor replaceDeployExecutor;

    public PrestoDeployHandler(BaseDeployExecutor replaceDeployExecutor) {
        this.replaceDeployExecutor = replaceDeployExecutor;
    }

    @Override
    public void handle(ComparisonJob comparisonJob, DeployInfo deployInfo) {
        String strategy = Optional.ofNullable(deployInfo.getStrategy()).orElse(DeployStrategyEnum.REPLACE.name());
        AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
        Map<String, Map<String, Object>> transformMap = appConf.getTransform();
        JobEnv jobEnv = BeanUtils.map2Bean(appConf.getEnv(), JobEnv.class);
        if (DeployStrategyEnum.REPLACE.name().equalsIgnoreCase(strategy)) {
            // 基于sql 实现数据补偿
            doByPrestoSql(jobEnv, transformMap);
        }
    }

    private void doByPrestoSql(JobEnv jobEnv,
                               Map<String, Map<String, Object>> transformMap) {
        for (Map.Entry<String, Map<String, Object>> entry : transformMap.entrySet()) {
            PrestoInfo prestoInfo = PrestoUtils.getPrestoInfo(jobEnv, entry.getValue());
            String allSql = SqlGeneratorUtil.generateSql(prestoInfo, jobEnv);
            // todo
            String[] sqlArray = allSql.split(BaseConstant.Symbol.SEMICOLON);
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

}
