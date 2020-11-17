package com.github.thestyleofme.data.comparison.transform.pojo;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * @author siqi.hou@hand-china.com
 * @date 2020-11-16 21:06
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CatalogInfo {

    private String sourceDatasourceCode;
    private String targetDatasourceCode;
    private String sourceSchema;
    private String targetSchema;
    private String sourceTable;
    private String targetTable;
}
