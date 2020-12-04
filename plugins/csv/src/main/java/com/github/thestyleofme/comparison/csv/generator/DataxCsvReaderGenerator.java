package com.github.thestyleofme.comparison.csv.generator;

import java.util.*;

import com.github.thestyleofme.comparison.common.app.service.datax.BaseDataxReaderGenerator;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.domain.entity.Reader;
import com.github.thestyleofme.comparison.common.infra.annotation.DataxReaderType;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.csv.pojo.CsvInfo;
import com.github.thestyleofme.comparison.csv.pojo.DataxCsvReader;
import com.github.thestyleofme.comparison.csv.utils.CsvUtil;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import org.springframework.stereotype.Component;

/**
 * <p></p>
 *
 * @author hsq 2020/12/03 15:16
 * @since 1.0.0
 */
@DataxReaderType("CSV")
@Component
public class DataxCsvReaderGenerator implements BaseDataxReaderGenerator {

    @Override
    public Reader generate(Long tenantId, ComparisonJob comparisonJob, Map<String, Object> sinkMap, Integer syncType) {
        CsvInfo csvInfo = BeanUtils.map2Bean(sinkMap, CsvInfo.class);
        DataxCsvReader dataxCsvReader = new DataxCsvReader();
        if (Objects.nonNull(csvInfo)) {
            // 封装datax csv reader
            String jobCode = comparisonJob.getJobCode();
            List<ColMapping> colMappingList = CommonUtil.getColMappingList(comparisonJob);
            List<DataxCsvReader.Column> column = new ArrayList<>();
            for (int i = 0; i < colMappingList.size(); i++) {
                column.add(DataxCsvReader.Column.builder().index(i).build());
            }
            String dir = Optional.ofNullable(csvInfo.getPath())
                    .orElseGet(() -> CommonUtil.createDirPath(String.format("csv/%s_%s", comparisonJob.getTenantId(), jobCode)));
            String path = CsvUtil.getCsvPath(dir, comparisonJob.getTenantId(), jobCode, syncType);
            DataxCsvReader.Parameter parameter = DataxCsvReader.Parameter.builder()
                    .fieldDelimiter("\u0001")
                    .encoding("utf-8")
                    .path(new String[]{path})
                    .column(column)
                    .build();
            dataxCsvReader.setParameter(parameter);
            dataxCsvReader.setName("txtfilereader");
        }
        return dataxCsvReader;
    }


}
