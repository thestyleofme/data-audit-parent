package com.github.thestyleofme.data.comparison.transform.pojo;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * @author siqi.hou@hand-china.com
 * @date 2020-11-17 9:38
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PrestoInfo {
    private String clusterCode;
    private String dataSourceCode;
    private String coordinatorUrl;
    private String username;

    private String sourceDatasourceCode;
    private String targetDatasourceCode;
    private String sourceSchema;
    private String targetSchema;
    private String sourceTable;
    private String targetTable;
}
