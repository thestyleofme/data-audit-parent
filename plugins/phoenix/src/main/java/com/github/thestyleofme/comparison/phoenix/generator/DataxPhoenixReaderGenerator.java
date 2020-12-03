package com.github.thestyleofme.comparison.phoenix.generator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.app.service.datax.BaseDataxReaderGenerator;
import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.domain.entity.Reader;
import com.github.thestyleofme.comparison.common.infra.annotation.DataxReaderType;
import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.phoenix.pojo.DatasourceInfo;
import com.github.thestyleofme.comparison.phoenix.pojo.PhoenixDataxReader;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import org.springframework.stereotype.Component;

/**
 * <p></p>
 *
 * @author hsq 2020/12/03 16:15
 * @since 1.0.0
 */
@DataxReaderType("PHOENIX")
@Component
public class DataxPhoenixReaderGenerator implements BaseDataxReaderGenerator {
    public static final Pattern PHOENIX_JDBC_PATTERN = Pattern.compile("jdbc:phoenix:thin:url=(.*?)");
    public static final String PHOENIX_SERIALIZATION = ";serialization=";

    @Override
    public Reader generate(Long tenantId, ComparisonJob comparisonJob, Map<String, Object> sinkMap, Integer syncType) {
        // 生成phoenix查询sql
        String jobName = comparisonJob.getJobCode();
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(comparisonJob);
        String sql = genPhoenixQuerySql(colMappingList, jobName, syncType);
        // 封装datax phoenix reader
        return genDataxPhoenixReader(comparisonJob, sql);
    }

    private PhoenixDataxReader genDataxPhoenixReader(ComparisonJob comparisonJob, String sql) {
        AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
        Map<String, Object> phoenixMap = appConf.getSink().get("phoenix");
        String jdbcUrl = (String) phoenixMap.get(DatasourceInfo.FIELD_JDBC_URL);
        Matcher matcher = PHOENIX_JDBC_PATTERN.matcher(jdbcUrl);
        String queryServerAddress;
        if (matcher.matches()) {
            queryServerAddress = matcher.group(1);
        } else {
            throw new HandlerException(ErrorCode.JOB_PHOENIX_JDBC_URL_NOT_FOUND);
        }
        PhoenixDataxReader phoenixDataxReader = PhoenixDataxReader.builder()
                .queryServerAddress(queryServerAddress)
                .querySql(Collections.singletonList(sql))
                .build();
        if (queryServerAddress.contains(PHOENIX_SERIALIZATION)) {
            String serialization = queryServerAddress.substring(queryServerAddress.indexOf(PHOENIX_SERIALIZATION) + PHOENIX_SERIALIZATION.length() + 1);
            phoenixDataxReader.setSerialization(serialization);
        }
        return phoenixDataxReader;
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
