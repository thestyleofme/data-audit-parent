package com.github.thestyleofme.data.comparison.app.service.impl;

import java.util.stream.Collectors;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJobGroup;
import com.github.thestyleofme.data.comparison.api.dto.ComparisonJobGroupDTO;
import com.github.thestyleofme.data.comparison.app.service.ComparisonJobGroupService;
import com.github.thestyleofme.data.comparison.infra.converter.BaseComparisonJobGroupConvert;
import com.github.thestyleofme.data.comparison.infra.mapper.ComparisonJobGroupMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/17 10:43
 * @since 1.0.0
 */
@Service
@Slf4j
public class ComparisonJobGroupServiceImpl extends ServiceImpl<ComparisonJobGroupMapper, ComparisonJobGroup> implements ComparisonJobGroupService {

    @Override
    public IPage<ComparisonJobGroupDTO> list(Page<ComparisonJobGroup> page, ComparisonJobGroupDTO comparisonJobGroupDTO) {
        QueryWrapper<ComparisonJobGroup> queryWrapper = new QueryWrapper<>(
                BaseComparisonJobGroupConvert.INSTANCE.dtoToEntity(comparisonJobGroupDTO));
        Page<ComparisonJobGroup> entityPage = page(page, queryWrapper);
        final Page<ComparisonJobGroupDTO> dtoPage = new Page<>();
        org.springframework.beans.BeanUtils.copyProperties(entityPage, dtoPage);
        dtoPage.setRecords(entityPage.getRecords().stream()
                .map(BaseComparisonJobGroupConvert.INSTANCE::entityToDTO)
                .collect(Collectors.toList()));
        return dtoPage;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public ComparisonJobGroupDTO save(ComparisonJobGroupDTO comparisonJobGroupDTO) {
        ComparisonJobGroup comparisonJobGroup = BaseComparisonJobGroupConvert.INSTANCE.dtoToEntity(comparisonJobGroupDTO);
        this.saveOrUpdate(comparisonJobGroup);
        return BaseComparisonJobGroupConvert.INSTANCE.entityToDTO(comparisonJobGroup);
    }
}
