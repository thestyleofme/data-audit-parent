package com.github.thestyleofme.data.comparison.app.service.impl;

import java.util.stream.Collectors;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.thestyleofme.data.comparison.api.dto.ComparisonJobDTO;
import com.github.thestyleofme.data.comparison.app.service.ComparisonJobService;
import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;
import com.github.thestyleofme.data.comparison.infra.context.JobHandlerContext;
import com.github.thestyleofme.data.comparison.infra.converter.BaseComparisonJobConvert;
import com.github.thestyleofme.data.comparison.infra.handler.BaseJobHandler;
import com.github.thestyleofme.data.comparison.infra.handler.HandlerResult;
import com.github.thestyleofme.data.comparison.infra.mapper.ComparisonJobMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 11:13
 * @since 1.0.0
 */
@Service
@Slf4j
public class ComparisonJobServiceImpl extends ServiceImpl<ComparisonJobMapper, ComparisonJob> implements ComparisonJobService {

    private final JobHandlerContext jobHandlerContext;

    public ComparisonJobServiceImpl(JobHandlerContext jobHandlerContext) {
        this.jobHandlerContext = jobHandlerContext;
    }

    @Override
    public IPage<ComparisonJobDTO> list(Page<ComparisonJob> page, ComparisonJobDTO comparisonJobDTO) {
        QueryWrapper<ComparisonJob> queryWrapper = new QueryWrapper<>(
                BaseComparisonJobConvert.INSTANCE.dtoToEntity(comparisonJobDTO));
        Page<ComparisonJob> entityPage = page(page, queryWrapper);
        final Page<ComparisonJobDTO> dtoPage = new Page<>();
        BeanUtils.copyProperties(entityPage, dtoPage);
        dtoPage.setRecords(entityPage.getRecords().stream()
                .map(BaseComparisonJobConvert.INSTANCE::entityToDTO)
                .collect(Collectors.toList()));
        return dtoPage;
    }

    @Override
    public void execute(Long tenantId, Long id) {
        ComparisonJob comparisonJob = this.getOne(new QueryWrapper<>(ComparisonJob.builder().tenantId(tenantId).id(id).build()));
        BaseJobHandler jobHandler = jobHandlerContext.getJobHandler(comparisonJob.getEngineType());
        HandlerResult handlerResult = jobHandler.handle(comparisonJob);
        log.debug("result: {}", handlerResult);
    }
}
