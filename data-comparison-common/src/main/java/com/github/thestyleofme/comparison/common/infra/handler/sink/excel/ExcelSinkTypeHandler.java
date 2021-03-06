package com.github.thestyleofme.comparison.common.infra.handler.sink.excel;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.alibaba.excel.EasyExcelFactory;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.github.thestyleofme.comparison.common.app.service.sink.BaseSinkHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.SinkType;
import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.common.infra.utils.ExcelUtil;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/27 9:39
 * @since 1.0.0
 */
@Component
@SinkType("EXCEL")
@Slf4j
public class ExcelSinkTypeHandler implements BaseSinkHandler {

    @Override
    public void handle(ComparisonJob comparisonJob,
                       Map<String, Object> env,
                       Map<String, Object> sinkMap,
                       HandlerResult handlerResult) {
        ExcelInfo excelInfo = BeanUtils.map2Bean(sinkMap, ExcelInfo.class);
        String excelPath = getExcelPath(excelInfo, comparisonJob);
        String excelName = String.format("%s/%d_%s.xlsx", excelPath, comparisonJob.getTenantId(), comparisonJob.getJobCode());
        checkExcelFile(excelName);
        List<List<String>> sourceExcelHeader = ExcelUtil.getSourceExcelHeader(comparisonJob);
        List<List<String>> sourceToTargetHeader = ExcelUtil.getSourceToTargetHeader(comparisonJob);
        List<List<String>> targetExcelHeader = ExcelUtil.getTargetExcelHeader(comparisonJob);
        log.debug("output to excel[{}] start", excelName);
        ExcelWriter excelWriter = null;
        try {
            excelWriter = EasyExcelFactory.write(excelName).build();
            doSourceUnique(excelWriter, sourceExcelHeader, handlerResult);
            doTargetUnique(excelWriter, targetExcelHeader, handlerResult);
            doPkOrIndexSame(excelWriter, sourceToTargetHeader, handlerResult);
            doDifferent(excelWriter, sourceToTargetHeader, handlerResult);
        } finally {
            if (excelWriter != null) {
                excelWriter.finish();
            }
        }
        log.debug("output to excel[{}] end", excelName);
    }

    private String getExcelPath(ExcelInfo excelInfo, ComparisonJob comparisonJob) {
        String fileOutputPath = excelInfo.getOutputPath();
        if (StringUtils.isEmpty(fileOutputPath)) {
            // when sinkType=EXCEL, fileOutputPath default path: {project}/excel/
            fileOutputPath = ExcelUtil.getExcelPath(comparisonJob);
        }
        return fileOutputPath;
    }

    @SuppressWarnings("unchecked")
    @Override
    public BaseSinkHandler proxy() {
        return CommonUtil.createProxy(
                this.getClass().getClassLoader(),
                BaseSinkHandler.class,
                (proxy, method, args) -> {
                    try {
                        return method.invoke(this, args);
                    } catch (Exception e) {
                        // 抛异常需要将文件删除
                        Map<String, Object> sinkMap = (Map<String, Object>) args[2];
                        ExcelInfo excelInfo = BeanUtils.map2Bean(sinkMap, ExcelInfo.class);
                        ComparisonJob comparisonJob = (ComparisonJob) args[0];
                        String excelPath = getExcelPath(excelInfo, comparisonJob);
                        CommonUtil.deleteFile(comparisonJob, excelPath);
                        throw e;
                    }
                }
        );
    }

    private void checkExcelFile(String excelName) {
        File file = new File(excelName);
        if (file.exists()) {
            log.warn("the excel[{}] is exist, delete it first", excelName);
            try {
                Files.delete(file.toPath());
                log.debug("the excel[{}] successfully deleted", excelName);
            } catch (IOException e) {
                throw new HandlerException(ErrorCode.EXCEL_DELETE_ERROR, excelName);
            }
        }
    }

    private void doDifferent(ExcelWriter excelWriter, List<List<String>> sourceToTargetHeader, HandlerResult handlerResult) {
        // 生成excel数据 List<List<Object>>
        List<Map<String, Object>> differentDataList = handlerResult.getDifferentDataList();
        if (CollectionUtils.isEmpty(differentDataList)) {
            return;
        }
        List<List<Object>> pkOrIndexList = differentDataList.stream()
                .filter(Objects::nonNull)
                .map(linkedHashMap -> new ArrayList<>(linkedHashMap.values()))
                .collect(Collectors.toList());
        // 开始写 多sheet页
        WriteSheet writeSheet = EasyExcelFactory.writerSheet(
                3, "源表目标表部分字段数据不一样")
                .head(sourceToTargetHeader).build();
        excelWriter.write(pkOrIndexList, writeSheet);
    }

    private void doPkOrIndexSame(ExcelWriter excelWriter, List<List<String>> sourceToTargetHeader, HandlerResult handlerResult) {
        // 生成excel数据 List<List<Object>>
        List<List<Object>> pkOrIndexList = handlerResult.getPkOrIndexSameDataList().stream()
                .filter(Objects::nonNull)
                .map(linkedHashMap -> new ArrayList<>(linkedHashMap.values()))
                .collect(Collectors.toList());
        // 开始写 多sheet页
        WriteSheet writeSheet = EasyExcelFactory.writerSheet(
                2, "源表目标表主键或唯一索引一样数据却不一样")
                .head(sourceToTargetHeader).build();
        excelWriter.write(pkOrIndexList, writeSheet);
    }

    private void doTargetUnique(ExcelWriter excelWriter,
                                List<List<String>> targetExcelHeader,
                                HandlerResult handlerResult) {
        // 生成excel数据 List<List<Object>>
        List<List<Object>> targetDataList = handlerResult.getTargetUniqueDataList().stream()
                .filter(Objects::nonNull)
                .map(linkedHashMap -> new ArrayList<>(linkedHashMap.values()))
                .collect(Collectors.toList());
        // 开始写 多sheet页
        WriteSheet writeSheet = EasyExcelFactory.writerSheet(
                1, "仅目标表拥有")
                .head(targetExcelHeader).build();
        excelWriter.write(targetDataList, writeSheet);
    }

    private void doSourceUnique(ExcelWriter excelWriter,
                                List<List<String>> sourceExcelHeader,
                                HandlerResult handlerResult) {
        // 生成excel数据 List<List<Object>>
        List<List<Object>> sourceDataList = handlerResult.getSourceUniqueDataList().stream()
                .filter(Objects::nonNull)
                .map(linkedHashMap -> new ArrayList<>(linkedHashMap.values()))
                .collect(Collectors.toList());
        // 开始写 多sheet页
        WriteSheet writeSheet = EasyExcelFactory.writerSheet(
                0, "仅源表拥有")
                .head(sourceExcelHeader).build();
        excelWriter.write(sourceDataList, writeSheet);
    }
}
