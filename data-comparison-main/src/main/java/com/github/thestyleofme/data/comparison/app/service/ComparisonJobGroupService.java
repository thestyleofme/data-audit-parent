package com.github.thestyleofme.data.comparison.app.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJobGroup;
import com.github.thestyleofme.data.comparison.api.dto.ComparisonJobGroupDTO;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 11:12
 * @since 1.0.0
 */
public interface ComparisonJobGroupService extends IService<ComparisonJobGroup> {

    /**
     * 分页条件查询
     *
     * @param page                  分页
     * @param comparisonJobGroupDTO ComparisonJobGroupDTO
     * @return IPage<ComparisonJobGroupDTO>
     */
    IPage<ComparisonJobGroupDTO> list(Page<ComparisonJobGroup> page, ComparisonJobGroupDTO comparisonJobGroupDTO);

    /**
     * 保存
     *
     * @param comparisonJobGroupDTO ComparisonJobGroupDTO
     * @return ComparisonJobGroupDTO
     */
    ComparisonJobGroupDTO save(ComparisonJobGroupDTO comparisonJobGroupDTO);
}
