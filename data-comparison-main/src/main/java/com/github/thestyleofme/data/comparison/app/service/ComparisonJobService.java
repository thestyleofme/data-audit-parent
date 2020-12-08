package com.github.thestyleofme.data.comparison.app.service;

import javax.servlet.http.HttpServletResponse;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import com.github.thestyleofme.comparison.common.domain.DeployInfo;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.domain.entity.Reader;
import com.github.thestyleofme.data.comparison.api.dto.ComparisonJobDTO;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 11:12
 * @since 1.0.0
 */
public interface ComparisonJobService extends IService<ComparisonJob> {

    /**
     * 分页条件查询数据稽核job
     *
     * @param page             分页
     * @param comparisonJobDTO ComparisonJobDTO
     * @return IPage<PluginDTO>
     */
    IPage<ComparisonJobDTO> list(Page<ComparisonJob> page, ComparisonJobDTO comparisonJobDTO);

    /**
     * 执行数据稽核job
     *
     * @param tenantId  租户id
     * @param jobCode   jobCode
     * @param groupCode groupCode
     */
    void execute(Long tenantId, String jobCode, String groupCode);

    /**
     * 保存数据稽核job
     *
     * @param comparisonJobDTO ComparisonJobDTO
     * @return ComparisonJobDTO
     */
    ComparisonJobDTO save(ComparisonJobDTO comparisonJobDTO);

    /**
     * 执行数据补偿
     *
     * @param deployInfo 数据补偿信息
     */
    void deploy(DeployInfo deployInfo);

    /**
     * 生成不同来源的datax reader
     *
     * @param comparisonJob job任务
     * @param syncType      数据同步类型 0：插入；1：删除；2：更新
     * @return datax reader的内容
     */
    Reader getDataxReader(ComparisonJob comparisonJob, Integer syncType);

    /**
     * 下载稽核结果文件
     *
     * @param tenantId 租户id
     * @param jobId    jobId
     * @param response 返回zip文件
     */
    void download(Long tenantId, Long jobId, HttpServletResponse response);
}
