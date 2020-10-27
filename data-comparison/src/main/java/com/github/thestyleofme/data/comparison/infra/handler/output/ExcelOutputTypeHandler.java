package com.github.thestyleofme.data.comparison.infra.handler.output;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.alibaba.excel.EasyExcelFactory;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.fasterxml.jackson.core.type.TypeReference;
import com.github.thestyleofme.data.comparison.domain.entity.ColMapping;
import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;
import com.github.thestyleofme.data.comparison.infra.annotation.OutputType;
import com.github.thestyleofme.data.comparison.infra.exceptions.HandlerException;
import com.github.thestyleofme.data.comparison.infra.handler.HandlerResult;
import com.github.thestyleofme.data.comparison.infra.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
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
@OutputType("EXCEL")
@Slf4j
public class ExcelOutputTypeHandler implements BaseOutputHandler {

    @Override
    public void handle(ComparisonJob comparisonJob, HandlerResult handlerResult) {
        String fileOutputPath = comparisonJob.getFileOutputPath();
        if (StringUtils.isEmpty(fileOutputPath)) {
            throw new HandlerException("when outputType=EXCEL, fileOutputPath cannot be null");
        }
        String excelName = String.format("%s/%d_%s.xlsx", fileOutputPath, comparisonJob.getTenantId(), comparisonJob.getJobName());
        checkExcelFile(excelName);
        List<ColMapping> colMappingList = JsonUtil.toObj(comparisonJob.getColMapping(), new TypeReference<List<ColMapping>>() {
        });
        log.debug("output to excel[{}] start", excelName);
        ExcelWriter excelWriter = null;
        try {
            excelWriter = EasyExcelFactory.write(excelName).build();
            doSourceUnique(excelWriter, colMappingList, handlerResult);
            doTargetUnique(excelWriter, colMappingList, handlerResult);
            doPkOrIndexSame(excelWriter, colMappingList, handlerResult);
            doSame(excelWriter, colMappingList, handlerResult);
        } finally {
            if (excelWriter != null) {
                excelWriter.finish();
            }
        }
        log.debug("output to excel[{}] end", excelName);
    }

    private void checkExcelFile(String excelName) {
        File file = new File(excelName);
        if (file.exists()) {
            log.warn("the excel[{}] is exist, delete it first", excelName);
            try {
                Files.delete(file.toPath());
                log.debug("the excel[{}] successfully deleted", excelName);
            } catch (IOException e) {
                throw new HandlerException("excel[{}] delete error", excelName);
            }
        }
    }


    private void doSame(ExcelWriter excelWriter, List<ColMapping> colMappingList, HandlerResult handlerResult) {
        // 生成excel头 List<List<String>>
        List<List<String>> headerList = colMappingList.stream()
                .map(colMapping -> Collections.singletonList(
                        colMapping.getSourceCol().equals(colMapping.getTargetCol()) ? colMapping.getSourceCol() :
                                String.format("%s -> %s", colMapping.getSourceCol(), colMapping.getTargetCol())))
                .collect(Collectors.toList());
        // 生成excel数据 List<List<Object>>
        List<List<Object>> sameList = handlerResult.getSameDataList().stream()
                .filter(Objects::nonNull)
                .map(linkedHashMap -> new ArrayList<>(linkedHashMap.values()))
                .collect(Collectors.toList());
        // 开始写 多sheet页
        WriteSheet writeSheet = EasyExcelFactory.writerSheet(
                3, "源表目标表数据一样")
                .head(headerList).build();
        excelWriter.write(sameList, writeSheet);
    }

    private void doPkOrIndexSame(ExcelWriter excelWriter, List<ColMapping> colMappingList, HandlerResult handlerResult) {
        // 生成excel头 List<List<String>>
        List<List<String>> headerList = colMappingList.stream()
                .map(colMapping -> Collections.singletonList(
                        colMapping.getSourceCol().equals(colMapping.getTargetCol()) ? colMapping.getSourceCol() :
                                String.format("%s -> %s", colMapping.getSourceCol(), colMapping.getTargetCol())))
                .collect(Collectors.toList());
        // 生成excel数据 List<List<Object>>
        List<List<Object>> pkOrIndexList = handlerResult.getPkOrIndexSameDataList().stream()
                .filter(Objects::nonNull)
                .map(linkedHashMap -> new ArrayList<>(linkedHashMap.values()))
                .collect(Collectors.toList());
        // 开始写 多sheet页
        WriteSheet writeSheet = EasyExcelFactory.writerSheet(
                2, "源表目标表主键或唯一索引一样数据却不一样")
                .head(headerList).build();
        excelWriter.write(pkOrIndexList, writeSheet);
    }

    private void doTargetUnique(ExcelWriter excelWriter, List<ColMapping> colMappingList, HandlerResult handlerResult) {
        // 生成excel头 List<List<String>>
        List<List<String>> excelTargetHeaderList = colMappingList.stream()
                .map(colMapping -> Collections.singletonList(colMapping.getTargetCol()))
                .collect(Collectors.toList());
        // 生成excel数据 List<List<Object>>
        List<List<Object>> targetDataList = handlerResult.getTargetUniqueDataList().stream()
                .filter(Objects::nonNull)
                .map(linkedHashMap -> new ArrayList<>(linkedHashMap.values()))
                .collect(Collectors.toList());
        // 开始写 多sheet页
        WriteSheet writeSheet = EasyExcelFactory.writerSheet(
                1, "仅目标表拥有")
                .head(excelTargetHeaderList).build();
        excelWriter.write(targetDataList, writeSheet);
    }

    private void doSourceUnique(ExcelWriter excelWriter, List<ColMapping> colMappingList, HandlerResult handlerResult) {
        // 生成excel头 List<List<String>>
        List<List<String>> excelSourceHeaderList = colMappingList.stream()
                .map(colMapping -> Collections.singletonList(colMapping.getSourceCol()))
                .collect(Collectors.toList());
        // 生成excel数据 List<List<Object>>
        List<List<Object>> sourceDataList = handlerResult.getSourceUniqueDataList().stream()
                .filter(Objects::nonNull)
                .map(linkedHashMap -> new ArrayList<>(linkedHashMap.values()))
                .collect(Collectors.toList());
        // 开始写 多sheet页
        WriteSheet writeSheet = EasyExcelFactory.writerSheet(
                0, "仅源表拥有")
                .head(excelSourceHeaderList).build();
        excelWriter.write(sourceDataList, writeSheet);
    }
}
