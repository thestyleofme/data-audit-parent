package com.github.thestyleofme.deploy.presto.hanlder;

import com.github.thestyleofme.comparison.common.domain.AppConf;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * <p>
 * siqi.hou
 * </p>
 *
 * @author isaac 2020/11/20 10:10
 * @since 1.0.0
 */
@Slf4j
@Component
public class ReplaceDeployExecutor implements BaseDeployExecutor {

    @Override
    public void doSourceToTarget(AppConf appConf) {
        log.debug("==> doSourceToTarget deploy start");
    }

    @Override
    public void doTargetExtraData(AppConf appConf) {
        log.debug("==> doTargetExtraData target deploy start");
    }

    @Override
    public void doOnlySamePkOrIndexData(AppConf appConf) {
        log.debug("==> target doOnlySamePkOrIndexData deploy start");
    }

}
