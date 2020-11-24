package com.github.thestyleofme.comparison.source.pojo;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/11 14:58
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableInfo {

    private String sourceDatasourceCode;
    private String targetDatasourceCode;
    private String sourceSchema;
    private String targetSchema;
    private String sourceTable;
    private String targetTable;

}
