package com.github.thestyleofme.deploy.presto.hanlder;

import com.github.thestyleofme.comparison.common.domain.AppConf;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author siqi.hou@hand-china.com
 * @date 2020-11-19 14:48
 */
@Slf4j
@Component
public class ReplaceDeployExecutor extends BaseDeployExecutor {

    @Override
    void doSourceToTarget(AppConf appConf) {
        log.debug("==> doSourceToTarget deploy start");

    }

    @Override
    void doTargetExtraData(AppConf appConf) {

        log.debug("==> doTargetExtraData target deploy start");

    }

    @Override
    void doOnlySamePkOrIndexData(AppConf appConf) {
        log.debug("==> target doOnlySamePkOrIndexData deploy start");

    }
}
