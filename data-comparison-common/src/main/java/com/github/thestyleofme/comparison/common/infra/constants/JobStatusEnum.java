package com.github.thestyleofme.comparison.common.infra.constants;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/18 10:15
 * @since 1.0.0
 */
public enum JobStatusEnum {

    /**
     * NEW 新建
     */
    NEW,
    /**
     * 数据稽核处理中
     */
    STARTING,
    /**
     * 之所以有这个状态，是因为有些任务预比对成功后，不执行具体稽核，故给此状态以示区分
     * 预比对失败，状态为AUDIT_FAILED
     */
    PRE_AUDIT_SUCCESS,
    AUDIT_SUCCESS,
    AUDIT_FAILED,
    DEPLOY_SUCCESS,
    DEPLOY_FAILED,
    ;
}
