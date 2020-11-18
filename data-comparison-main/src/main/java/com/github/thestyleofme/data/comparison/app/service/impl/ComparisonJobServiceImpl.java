package com.github.thestyleofme.data.comparison.app.service.impl;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.alibaba.excel.EasyExcelFactory;
import com.alibaba.excel.ExcelReader;
import com.alibaba.excel.read.metadata.ReadSheet;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.thestyleofme.comparison.common.app.service.sink.BaseSinkHandler;
import com.github.thestyleofme.comparison.common.app.service.sink.SinkHandlerProxy;
import com.github.thestyleofme.comparison.common.app.service.source.BaseSourceHandler;
import com.github.thestyleofme.comparison.common.app.service.source.SourceDataMapping;
import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.app.service.transform.TransformHandlerProxy;
import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.TransformInfo;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJobGroup;
import com.github.thestyleofme.comparison.common.infra.constants.CommonConstant;
import com.github.thestyleofme.comparison.common.infra.constants.JobStatusEnum;
import com.github.thestyleofme.comparison.common.infra.excel.CommonExcelListener;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.common.infra.utils.ExcelUtil;
import com.github.thestyleofme.comparison.common.infra.utils.HandlerUtil;
import com.github.thestyleofme.data.comparison.api.dto.ComparisonJobDTO;
import com.github.thestyleofme.data.comparison.app.service.ComparisonJobGroupService;
import com.github.thestyleofme.data.comparison.app.service.ComparisonJobService;
import com.github.thestyleofme.data.comparison.infra.context.JobHandlerContext;
import com.github.thestyleofme.data.comparison.infra.converter.BaseComparisonJobConvert;
import com.github.thestyleofme.data.comparison.infra.mapper.ComparisonJobMapper;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

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
    private final ComparisonJobGroupService comparisonJobGroupService;
    private final ReentrantLock lock = new ReentrantLock();

    public ComparisonJobServiceImpl(JobHandlerContext jobHandlerContext,
                                    ComparisonJobGroupService comparisonJobGroupService) {
        this.jobHandlerContext = jobHandlerContext;
        this.comparisonJobGroupService = comparisonJobGroupService;
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
    @Transactional(rollbackFor = Exception.class)
    public void execute(Long tenantId, String jobCode, String groupCode) {
        CompletableFuture.supplyAsync(() -> {
            // 要么执行组下的任务，要么执行某一个job 两者取其一
            if (!StringUtils.isEmpty(groupCode)) {
                doGroupJob(tenantId, groupCode);
                return false;
            }
            if (StringUtils.isEmpty(jobCode)) {
                // 都没传 直接抛异常
                throw new HandlerException("hdsp.xadt.error.both.jobCode.groupCode.is_null");
            }
            ComparisonJob comparisonJob = getOne(new QueryWrapper<>(ComparisonJob.builder()
                    .tenantId(tenantId).jobCode(jobCode).build()));
            doJob(comparisonJob);
            return true;
        });
    }

    private void doJob(ComparisonJob comparisonJob) {
        // 检验任务是否正在执行 以及设置开始执行状态
        doBefore(comparisonJob);
        try {
            AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
            // env
            Map<String, Object> env = appConf.getEnv();
            // source
            SourceDataMapping sourceDataMapping = doSource(appConf, env, comparisonJob);
            // transform
            HandlerResult handlerResult = doTransform(appConf, env, sourceDataMapping, comparisonJob);
            // sink
            doSink(appConf, env, comparisonJob, handlerResult);
        } catch (Exception e) {
            updateJobStatus(comparisonJob, JobStatusEnum.FAILED.name(), HandlerUtil.getMessage(e));
        } finally {
            updateJobStatus(comparisonJob, JobStatusEnum.SUCCESS.name(), null);
        }
    }

    private void updateJobStatus(ComparisonJob comparisonJob, String status, String errorMag) {
        comparisonJob.setStatus(status);
        comparisonJob.setErrorMsg(errorMag);
        long millis = Duration.between(comparisonJob.getStartTime(), LocalDateTime.now()).toMillis();
        comparisonJob.setExecuteTime(HandlerUtil.timestamp2String(millis));
        updateById(comparisonJob);
        if (StringUtils.isEmpty(errorMag)) {
            update(Wrappers.<ComparisonJob>lambdaUpdate()
                    .set(ComparisonJob::getErrorMsg, null)
                    .eq(ComparisonJob::getJobId, comparisonJob.getJobId()));
        }
    }

    private void doBefore(ComparisonJob comparisonJob) {
        lock.lock();
        try {
            // 任务正在运行则抛异常
            if (comparisonJob.getStatus().equals(JobStatusEnum.STARTING.name())) {
                throw new HandlerException("hdsp.xadt.error.job[%d_%s].is_staring",
                        comparisonJob.getTenantId(), comparisonJob.getJobCode());
            }
            // 开始执行 状态更新
            comparisonJob.setStatus(JobStatusEnum.STARTING.name());
            comparisonJob.setStartTime(LocalDateTime.now());
            comparisonJob.setExecuteTime(null);
            update(Wrappers.<ComparisonJob>lambdaUpdate()
                    .set(ComparisonJob::getExecuteTime, null)
                    .eq(ComparisonJob::getJobId, comparisonJob.getJobId()));
            updateById(comparisonJob);
        } finally {
            lock.unlock();
        }
    }

    private void doSink(AppConf appConf,
                        Map<String, Object> env,
                        ComparisonJob comparisonJob,
                        HandlerResult handlerResult) {
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

    private HandlerResult doTransform(AppConf appConf,
                                      Map<String, Object> env,
                                      SourceDataMapping sourceDataMapping,
                                      ComparisonJob comparisonJob) {
        HandlerResult handlerResult = null;
        for (Map.Entry<String, Map<String, Object>> entry : appConf.getTransform().entrySet()) {
            String key;
            TransformInfo transformInfo = BeanUtils.map2Bean(entry.getValue(), TransformInfo.class);
            if (Objects.isNull(transformInfo) || StringUtils.isEmpty(transformInfo.getType())) {
                // 如presto类型
                key = entry.getKey().toUpperCase();
            } else {
                // 如布隆过滤器类型
                key = String.format(CommonConstant.CONTACT, entry.getKey().toUpperCase(), transformInfo.getType());
            }
            BaseTransformHandler transformHandler = jobHandlerContext.getTransformHandler(key);
            // 可对BaseTransformHandler创建代理
            TransformHandlerProxy transformHandlerProxy = jobHandlerContext.getTransformHandleHook(key);
            if (transformHandlerProxy != null) {
                transformHandler = transformHandlerProxy.proxy(transformHandler);
            }
            handlerResult = transformHandler.handle(comparisonJob, env, entry.getValue(), sourceDataMapping);
        }
        return handlerResult;
    }

    private SourceDataMapping doSource(AppConf appConf,
                                       Map<String, Object> env,
                                       ComparisonJob comparisonJob) {
        SourceDataMapping sourceDataMapping = null;
        for (Map.Entry<String, Map<String, Object>> entry : appConf.getSource().entrySet()) {
            BaseSourceHandler sourceHandler = jobHandlerContext.getSourceHandler(entry.getKey().toUpperCase());
            sourceDataMapping = sourceHandler.handle(comparisonJob, env, entry.getValue());
        }
        return sourceDataMapping;
    }

    private void doGroupJob(Long tenantId, String groupCode) {
        ComparisonJobGroup jobGroup = comparisonJobGroupService.getOne(tenantId, groupCode);
        List<ComparisonJob> jobList = list(new QueryWrapper<>(ComparisonJob.builder()
                .tenantId(tenantId).groupCode(groupCode).build()));
        for (ComparisonJob comparisonJob : jobList) {
            // 将group中的信息写到job的全局参数env中 后续job执行或许用的上
            AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
            JobEnv jobEnv = BeanUtils.map2Bean(appConf.getEnv(), JobEnv.class);
            // 拷贝group信息到env
            org.springframework.beans.BeanUtils.copyProperties(jobGroup, jobEnv);
            appConf.setEnv(BeanUtils.bean2Map(jobEnv));
            comparisonJob.setAppConf(JsonUtil.toJson(appConf));
            doJob(comparisonJob);
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public ComparisonJobDTO save(ComparisonJobDTO comparisonJobDTO) {
        ComparisonJob comparisonJob = BaseComparisonJobConvert.INSTANCE.dtoToEntity(comparisonJobDTO);
        saveOrUpdate(comparisonJob);
        return BaseComparisonJobConvert.INSTANCE.entityToDTO(comparisonJob);
    }

    @Override
    public void deploy(Long tenantId, String jobCode, String groupCode) {
        CompletableFuture.supplyAsync(() -> {
            // 要么执行组下的任务，要么执行某一个job 两者取其一
            if (!StringUtils.isEmpty(groupCode)) {
                doGroupJobDeploy(tenantId, groupCode);
                return false;
            }
            if (StringUtils.isEmpty(jobCode)) {
                // 都没传 直接抛异常
                throw new HandlerException("hdsp.xadt.error.both.jobCode.groupCode.is_null");
            }
            ComparisonJob comparisonJob = getOne(new QueryWrapper<>(ComparisonJob.builder()
                    .tenantId(tenantId).jobCode(jobCode).build()));
            doJobDeploy(comparisonJob);
            return true;
        });
    }

    private void doJobDeploy(ComparisonJob comparisonJob) {
        // excel 路径
        String excelPath = ExcelUtil.getExcelPath(comparisonJob);
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(comparisonJob);
        List<List<String>> sourceExcelHeader = ExcelUtil.getSourceExcelHeader(colMappingList);
        ExcelReader excelReader = null;
        try {
            excelReader = EasyExcelFactory.read(excelPath).build();
            ReadSheet readSheet1 =
                    EasyExcelFactory.readSheet(0)
                            .head(ExcelUtil.getSourceExcelHeader(comparisonJob))
                            .registerReadListener(new CommonExcelListener<List<Object>>(sourceExcelHeader))
                            .build();
            excelReader.read(readSheet1);
        } finally {
            if (excelReader != null) {
                excelReader.finish();
            }
        }
    }

    private void doGroupJobDeploy(Long tenantId, String groupCode) {
        List<ComparisonJob> jobList = list(new QueryWrapper<>(ComparisonJob.builder()
                .tenantId(tenantId).groupCode(groupCode).build()));
        for (ComparisonJob comparisonJob : jobList) {
            doJobDeploy(comparisonJob);
        }
    }


}
