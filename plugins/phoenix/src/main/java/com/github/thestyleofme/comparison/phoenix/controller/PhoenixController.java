package com.github.thestyleofme.comparison.phoenix.controller;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.phoenix.pojo.DatasourceInfo;
import com.github.thestyleofme.comparison.phoenix.pojo.PhoenixDataxReaderInfo;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/25 15:55
 * @since 1.0.0
 */
@RestController("phoenixController.v1")
@RequestMapping("/v1/{organizationId}/phoenix")
@Slf4j
public class PhoenixController {

    public static final Pattern PHOENIX_JDBC_PATTERN = Pattern.compile("jdbc:phoenix:thin:url=(.*?)");
    public static final String PHOENIX_SERIALIZATION = ";serialization=";

    @ApiOperation(value = "生成phoenix datax reader")
    @PostMapping("/datax-reader")
    public PhoenixDataxReaderInfo getDataxPhoenixReader(@PathVariable(name = "organizationId") Long tenantId,
                                                        @RequestBody ComparisonJob comparisonJob,
                                                        @RequestParam(value = "rowType", required = false, defaultValue = "0") Integer rowType) {
        // 生成phoenix查询sql
        String jobName = comparisonJob.getJobCode();
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(comparisonJob);
        String sql = genPhoenixQuerySql(colMappingList, jobName, rowType);
        // 封装datax phoenix reader
        return genDataxPhoenixReader(comparisonJob, sql);
    }

    private PhoenixDataxReaderInfo genDataxPhoenixReader(ComparisonJob comparisonJob, String sql) {
        AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
        Map<String, Object> phoenixMap = appConf.getSink().get("phoenix");
        String jdbcUrl = (String) phoenixMap.get(DatasourceInfo.FIELD_JDBC_URL);
        Matcher matcher = PHOENIX_JDBC_PATTERN.matcher(jdbcUrl);
        String queryServerAddress;
        if (matcher.matches()) {
            queryServerAddress = matcher.group(1);
        } else {
            throw new HandlerException("hdsp.xadt.error.job.phoenix.jdbcUrl");
        }
        PhoenixDataxReaderInfo phoenixDataxReaderInfo = PhoenixDataxReaderInfo.builder()
                .queryServerAddress(queryServerAddress)
                .querySql(Collections.singletonList(sql))
                .build();
        if (queryServerAddress.contains(PHOENIX_SERIALIZATION)) {
            String serialization = queryServerAddress.substring(queryServerAddress.indexOf(PHOENIX_SERIALIZATION) + PHOENIX_SERIALIZATION.length() + 1);
            phoenixDataxReaderInfo.setSerialization(serialization);
        }
        return phoenixDataxReaderInfo;
    }

    private String genPhoenixQuerySql(List<ColMapping> colMappingList, String jobName, Integer rowType) {
        String selectColumns = colMappingList.stream()
                .map(colMapping -> String.format("\"0\".\"%s\"", colMapping.getSourceCol()))
                .collect(Collectors.joining(", "));
        StringBuilder stringBuilder = new StringBuilder("SELECT ");
        stringBuilder.append(selectColumns).append(" FROM data_audit.audit_result(");
        String fromColumns = colMappingList.stream()
                .map(colMapping -> String.format("\"0\".\"%s\" VARCHAR", colMapping.getSourceCol()))
                .collect(Collectors.joining(", "));
        stringBuilder.append(fromColumns).append(")");
        stringBuilder.append("WHERE \"-1\".job_name='").append(jobName).append("' ");
        stringBuilder.append("AND \"-1\".row_type=").append(rowType);
        return stringBuilder.toString();
    }
}
