package com.github.thestyleofme.deploy.presto.hanlder;

import com.github.thestyleofme.comparison.common.domain.AppConf;

/**
 * @author siqi.hou
 * @date 2020-11-19 14:41
 */
public interface BaseDeployExecutor {

    /**
     * 执行数据补偿
     *
     * @param appConf AppConf
     */
    default void doDeploy(AppConf appConf) {
        doSourceToTarget(appConf);
        doTargetExtraData(appConf);
        doOnlySamePkOrIndexData(appConf);
    }

    /**
     * 来源表数据补偿到目标表
     *
     * @param appConf AppConf
     */
    void doSourceToTarget(AppConf appConf);

    /**
     * 目标表多余的数据处理
     *
     * @param appConf AppConf
     */
    void doTargetExtraData(AppConf appConf);

    /**
     * 对仅主键或索引相同的数据进行补偿
     *
     * @param appConf AppConf
     */
    void doOnlySamePkOrIndexData(AppConf appConf);
}
