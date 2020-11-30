package com.github.thestyleofme.comparison.common.infra.utils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.constants.CommonConstant;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/27 10:44
 * @since 1.0.0
 */
@Slf4j
public class ExcelUtil {

    private ExcelUtil() {
    }

    public static String getExcelPath(ComparisonJob comparisonJob) {
        AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
        Map<String, Map<String, Object>> sinkMap = appConf.getSink();
        if (sinkMap.containsKey(CommonConstant.Sink.EXCEL)) {
            Object outputPath = sinkMap.get(CommonConstant.Sink.EXCEL).get("outputPath");
            if (outputPath == null) {
                outputPath = ExcelUtil.class.getResource("/").getPath() + "/excel";
            }
            return String.format("%s/%d_%s.xlsx", outputPath,
                    comparisonJob.getTenantId(), comparisonJob.getJobCode());
        }
        throw new HandlerException("hdsp.xadt.error.cannot.find.excel.path");
    }

    public static List<List<String>> getTargetExcelHeader(List<ColMapping> colMappingList) {
        return colMappingList.stream()
                .map(colMapping -> Collections.singletonList(colMapping.getTargetCol()))
                .collect(Collectors.toList());
    }

    public static List<List<String>> getTargetExcelHeader(ComparisonJob comparisonJob) {
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(comparisonJob);
        return colMappingList.stream()
                .map(colMapping -> Collections.singletonList(colMapping.getTargetCol()))
                .collect(Collectors.toList());
    }

    public static List<List<String>> getSourceExcelHeader(List<ColMapping> colMappingList) {
        return colMappingList.stream()
                .map(colMapping -> Collections.singletonList(colMapping.getSourceCol()))
                .collect(Collectors.toList());
    }

    public static List<List<String>> getSourceExcelHeader(ComparisonJob comparisonJob) {
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(comparisonJob);
        return colMappingList.stream()
                .map(colMapping -> Collections.singletonList(colMapping.getSourceCol()))
                .collect(Collectors.toList());
    }


    public static List<List<String>> getSourceToTargetHeader(ComparisonJob comparisonJob) {
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(comparisonJob);
        return colMappingList.stream()
                .map(colMapping -> Collections.singletonList(
                        colMapping.getSourceCol().equals(colMapping.getTargetCol()) ? colMapping.getSourceCol() :
                                String.format("%s -> %s", colMapping.getSourceCol(), colMapping.getTargetCol())))
                .collect(Collectors.toList());
    }

}
