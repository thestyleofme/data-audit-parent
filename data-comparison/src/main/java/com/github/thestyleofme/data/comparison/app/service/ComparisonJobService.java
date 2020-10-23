package com.github.thestyleofme.data.comparison.app.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import com.github.thestyleofme.data.comparison.api.dto.ComparisonJobDTO;
import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;

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
     * @param tenantId 租户id
     * @param id       job id
     */
    void execute(Long tenantId, Long id);

}
