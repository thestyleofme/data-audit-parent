package com.github.thestyleofme.data.comparison.app.service.impl;

import java.util.Map;
import java.util.stream.Collectors;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.thestyleofme.comparison.common.api.dto.ComparisonJobDTO;
import com.github.thestyleofme.comparison.common.app.service.sink.BaseSinkHandler;
import com.github.thestyleofme.comparison.common.app.service.sink.SinkHandlerProxy;
import com.github.thestyleofme.comparison.common.app.service.source.BaseSourceHandler;
import com.github.thestyleofme.comparison.common.app.service.source.SourceDataMapping;
import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.app.service.transform.TransformHandlerProxy;
import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.ComparisonJob;
import com.github.thestyleofme.comparison.common.domain.TransformInfo;
import com.github.thestyleofme.comparison.common.infra.converter.BaseComparisonJobConvert;
import com.github.thestyleofme.data.comparison.app.service.ComparisonJobService;
import com.github.thestyleofme.data.comparison.infra.context.JobHandlerContext;
import com.github.thestyleofme.data.comparison.infra.mapper.ComparisonJobMapper;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
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
        org.springframework.beans.BeanUtils.copyProperties(entityPage, dtoPage);
        dtoPage.setRecords(entityPage.getRecords().stream()
                .map(BaseComparisonJobConvert.INSTANCE::entityToDTO)
                .collect(Collectors.toList()));
        return dtoPage;
    }

    @Override
    public void execute(Long tenantId, Long id) {
        ComparisonJob comparisonJob = this.getOne(new QueryWrapper<>(ComparisonJob.builder()
                .tenantId(tenantId).id(id).build()));
        AppConf appConf = JsonUtil.toObj(comparisonJob.getApplicationConf(), AppConf.class);
        // sink
        Map<String, Object> env = appConf.getEnv();
        // source
        SourceDataMapping sourceDataMapping = null;
        for (Map.Entry<String, Map<String, Object>> entry : appConf.getSource().entrySet()) {
            BaseSourceHandler sourceHandler = jobHandlerContext.getSourceHandler(entry.getKey().toUpperCase());
            sourceDataMapping = sourceHandler.handle(comparisonJob, env, entry.getValue());
        }
        // transform
        HandlerResult handlerResult = null;
        for (Map.Entry<String, Map<String, Object>> entry : appConf.getTransform().entrySet()) {
            TransformInfo transformInfo = BeanUtils.map2Bean(entry.getValue(), TransformInfo.class);
            String key = entry.getKey().toUpperCase() + "_" + transformInfo.getType();
            BaseTransformHandler transformHandler = jobHandlerContext.getTransformHandler(key);
            // 可对BaseTransformHandler创建代理
            TransformHandlerProxy transformHandlerProxy = jobHandlerContext.getTransformHandleHook(key);
            if (transformHandlerProxy != null) {
                transformHandler = transformHandlerProxy.proxy(transformHandler);
            }
            handlerResult = transformHandler.handle(comparisonJob, env, sourceDataMapping);
        }
        // sink
        for (Map.Entry<String, Map<String, Object>> entry : appConf.getSink().entrySet()) {
            String key = entry.getKey().toUpperCase();
            BaseSinkHandler baseSinkHandler = jobHandlerContext.getSinkHandler(key);
            // 可对BaseSinkHandler创建代理
            SinkHandlerProxy sinkHandlerProxy = jobHandlerContext.getSinkHandlerProxy(key);
            if (sinkHandlerProxy != null) {
                baseSinkHandler = sinkHandlerProxy.proxy(baseSinkHandler);
            }
            baseSinkHandler.handle(comparisonJob, env, entry.getValue(), handlerResult);
        }
    }

    @Override
    public ComparisonJobDTO save(ComparisonJobDTO comparisonJobDTO) {
        // todo 校验
        ComparisonJob comparisonJob = BaseComparisonJobConvert.INSTANCE.dtoToEntity(comparisonJobDTO);
        this.saveOrUpdate(comparisonJob);
        return BaseComparisonJobConvert.INSTANCE.entityToDTO(comparisonJob);
    }
}
