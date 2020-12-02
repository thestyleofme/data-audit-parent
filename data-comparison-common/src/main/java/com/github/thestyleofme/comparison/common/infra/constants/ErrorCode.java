package com.github.thestyleofme.comparison.common.infra.constants;

/**
 * <p></p>
 *
 * @author hsq 2020/12/01 10:14
 * @since 1.0.0
 */
public interface ErrorCode {
    /**
     * 未找到excel文件路径
     */
    String EXCEL_PATH_NOT_FOUND = "hdsp.xadt.err.cannot.find.excel.path";
    /**
     * 删除excel失败
     */
    String EXCEL_DELETE_ERROR = "hdsp.xadt.err.excel.delete";
    /**
     * 数据稽核任务状态[{0}]不允许执行
     */
    String DEPLOY_STATUS_NOT_SUCCESS = "hdsp.xadt.err.deploy.status_not_success";
    /**
     * joinMapping条件为null
     */
    String DEPLOY_JOIN_CONDITION_IS_NULL = "hdsp.xadt.err.deploy.join_condition.is_null";
    /**
     * 基于excel做数据稽核的数据源未找到
     */
    String DEPLOY_EXCEL_DATASOURCE_NOT_FOUND = "hdsp.xadt.err.deploy.excel.datasource.not_found";
    /**
     * transformType类型[{0}]的实体类已存在
     */
    String HANDLER_TRANSFORM_TYPE_EXIST = "hdsp.xadt.err.handler.transformType.exist";
    /**
     * json中未找到transform节点
     */
    String TRANSFORM_IS_NULL = "hdsp.xadt.err.transform.is_null";
    /**
     * handlerResult为null
     */
    String HANDLER_RESULT_IS_NULL = "hdsp.xadt.err.handlerResult.is_null";
    /**
     * 两个属性值都为null
     */
    String BATH_PROPERTIES_IS_NULL = "hdsp.xadt.err.both.null";
    /**
     * preTransform实体类未找到
     */
    String PRE_TRANSFORM_CLASS_NOT_FOUND = "hdsp.xadt.err.preTransform.class.not_found";
    /**
     * 稽核任务组{0}不存在
     */
    String JOB_GROUP_CODE_NOT_EXIST = "hdsp.xadt.err.comparison.job_group.not_exist";
    /**
     * jdbcUrl未找到
     */
    String JOB_PHOENIX_JDBC_URL_NOT_FOUND = "hdsp.xadt.err.job.phoenix.jdbc_url";
    /**
     * phoenix 建库/表/序列号失败
     */
    String PHOENIX_CREATE_ERROR = "hdsp.xadt.err.phoenix.create.schema.table.sequence";
    /**
     * phoenix 执行sql语句失败
     */
    String PHOENIX_EXECUTE_ERROR = "hdsp.xadt.err.phoenix.execute.sql";
    /**
     * presto 解析url失败
     */
    String PRESTO_ANALYSIS_URL_ERROR = "hdsp.xadt.err.presto.analysis_url";
    /**
     * presto jdbc执行失败
     */
    String PRESTO_JDBC_EXECUTE_ERROR = "hdsp.xadt.err.presto.jdbc";
    /**
     * 未指定主键或唯一索引
     */
    String PRESTO_NOT_SUPPORT = "hdsp.xadt.err.presto.not_support";
    /**
     * presto 结果集未找到
     */
    String PRESTO_RESULT_NOT_FOUND = "hdsp.xadt.err.presto.result.not_support";
    /**
     * presto 保存catalog失败
     */
    String PRESTO_SAVE_CATALOG_ERROR = "hdsp.xadt.err.presto.catalog.save.error";
    /**
     * presto 删除catalog失败
     */
    String PRESTO_DELETE_CATALOG_ERROR = "hdsp.xadt.err.presto.catalog.delete.error";
    /**
     * 预处理信息：跳过数据稽核流程
     */
    String PRE_TRANSFORM_SKIP_INFO = "hdsp.xadt.info.pre.transform.skip.conditions.match";

    /**
     * 预处理执行失败，未获取到正确执行结果
     */
    String PRE_TRANSFORM_RESULT_NOT_FOUND = "hdsp.xadt.err.pre_transform.not_fount_result";

    // =========================== context ============================//
    /**
     * [{0}]代理对象已经存在
     */
    String HANDLER_PROXY_IS_EXIST = "hdsp.xadt.err.comparison.job.handler.proxy.is_exist";
    /**
     * [{0}]类型对象不存在
     */
    String HANDLER_TYPE_NOT_EXIST = "hdsp.xadt.err.comparison.job.handler.type.not.exist";
    /**
     * [{0}]类型对象已经存在
     */
    String HANDLER_TYPE_IS_EXIST = "hdsp.xadt.err.comparison.job.handler.type.is_exist";


}
