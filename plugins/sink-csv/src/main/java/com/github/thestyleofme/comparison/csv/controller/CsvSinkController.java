package com.github.thestyleofme.comparison.csv.controller;

import java.util.*;

import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.csv.pojo.CsvInfo;
import com.github.thestyleofme.comparison.csv.pojo.DataxCsvReader;
import com.github.thestyleofme.comparison.csv.utils.CsvUtil;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

/**
 * <p></p>
 *
 * @author hsq 2020/12/02 17:47
 * @since 1.0.0
 */
@RestController("csvSinkController.v1")
@RequestMapping("/v1/{organizationId}/csv")
@Slf4j
public class CsvSinkController {
    @ApiOperation(value = "生成phoenix datax reader")
    @PostMapping("/datax-reader")
    public DataxCsvReader getDataxPhoenixReader(@PathVariable(name = "organizationId") Long tenantId,
                                                @RequestBody ComparisonJob comparisonJob,
                                                @RequestParam(value = "type", required = false, defaultValue = "0") Integer type) {
        AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
        CsvInfo csvInfo = null;
        for (Map.Entry<String, Map<String, Object>> entry : appConf.getSink().entrySet()) {
            if ("csv".equalsIgnoreCase(entry.getKey())) {
                csvInfo = BeanUtils.map2Bean(entry.getValue(), CsvInfo.class);
            }
        }
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
                    .orElseGet(() -> String.format("csv/%s_%s", comparisonJob.getTenantId(), jobCode));
            String path = CsvUtil.getCsvPath(dir, comparisonJob.getTenantId(), jobCode, type);
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
