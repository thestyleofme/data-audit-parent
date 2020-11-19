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
    AUDIT_SUCCESS,
    AUDIT_FAILED,
    DEPLOY_SUCCESS,
    DEPLOY_FAILED;
}
