package com.github.thestyleofme.comparison.csv;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.app.service.sink.BaseSinkHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.SinkType;
import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.constants.RowTypeEnum;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.csv.pojo.CsvInfo;
import com.github.thestyleofme.comparison.csv.utils.CsvUtil;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * <p></p>
 *
 * @author hsq 2020/12/02 11:25
 * @since 1.0.0
 */
@Component
@SinkType("CSV")
@Slf4j
public class CsvSinkHandler implements BaseSinkHandler {

    @Override
    public void handle(ComparisonJob comparisonJob,
                       Map<String, Object> env,
                       Map<String, Object> sinkMap,
                       HandlerResult handlerResult) {
        // 获取文件夹路径
        CsvInfo csvInfo = BeanUtils.map2Bean(sinkMap, CsvInfo.class);
        if (StringUtils.isEmpty(csvInfo.getPath())) {
            String dirPath = CommonUtil.createDirPath(String.format("csv/%s_%s", comparisonJob.getTenantId(),
                    comparisonJob.getJobCode()));
            csvInfo.setPath(dirPath);
        }
        // 将数据写入csv
        writeToCsv(comparisonJob, csvInfo, handlerResult);
    }

    private void writeToCsv(ComparisonJob comparisonJob,
                            CsvInfo csvInfo,
                            HandlerResult handlerResult) {
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(comparisonJob);
        writeToInsertCsv(comparisonJob, colMappingList, csvInfo, handlerResult.getSourceUniqueDataList());
        writeToDeleteCsv(comparisonJob, colMappingList, csvInfo, handlerResult.getTargetUniqueDataList());
        writeToUpdateCsv(comparisonJob, colMappingList, csvInfo, handlerResult.getPkOrIndexSameDataList());
    }

    private void writeToInsertCsv(ComparisonJob comparisonJob,
                                  List<ColMapping> colMappingList,
                                  CsvInfo csvInfo,
                                  List<Map<String, Object>> dataList) {
        String path = CsvUtil.getCsvPath(csvInfo.getPath(), comparisonJob.getTenantId(), comparisonJob.getJobCode(),
                RowTypeEnum.INSERT.getRawType());
        List<String[]> csvDataList = fillCsvList(colMappingList, dataList);
        doWrite(path, csvDataList);
    }

    private void writeToDeleteCsv(ComparisonJob comparisonJob,
                                  List<ColMapping> colMappingList,
                                  CsvInfo csvInfo,
                                  List<Map<String, Object>> dataList) {
        String path = CsvUtil.getCsvPath(csvInfo.getPath(), comparisonJob.getTenantId(), comparisonJob.getJobCode(), RowTypeEnum.DELETED.getRawType());
        List<String[]> csvDataList = fillCsvList(colMappingList, dataList);
        doWrite(path, csvDataList);
    }

    private void writeToUpdateCsv(ComparisonJob comparisonJob,
                                  List<ColMapping> colMappingList,
                                  CsvInfo csvInfo,
                                  List<Map<String, Object>> dataList) {
        String path = CsvUtil.getCsvPath(csvInfo.getPath(), comparisonJob.getTenantId(), comparisonJob.getJobCode(),
                RowTypeEnum.UPDATED.getRawType());
        List<String[]> csvDataList = fillCsvList(colMappingList, dataList);
        doWrite(path, csvDataList);
    }

    private List<String[]> fillCsvList(List<ColMapping> colMappingList, List<Map<String, Object>> dataList) {
        List<String[]> csvDataList = new ArrayList<>();
        String[] colNames = colMappingList.stream().map(ColMapping::getSourceCol).toArray(String[]::new);
        csvDataList.add(colNames);
        List<String[]> data = dataList.stream()
                .map(map -> map.values().stream()
                        .map(o -> o == null ? "" : String.valueOf(o))
                        .toArray(String[]::new))
                .collect(Collectors.toList());
        csvDataList.addAll(data);
        return csvDataList;
    }

    private void doWrite(String path, List<String[]> data) {
        CSVWriterBuilder builder;
        try {
            builder = new CSVWriterBuilder(new FileWriter(path));
        } catch (IOException e) {
            throw new HandlerException(ErrorCode.CSV_PATH_NOT_FOUND, path);
        }
        try (ICSVWriter writer = builder.withSeparator('\u0001').build()) {
            writer.writeAll(data);
        } catch (IOException e) {
            // ignore
            log.warn("ICSVWriter IOException",e);
        }
    }


}
