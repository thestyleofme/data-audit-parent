package com.github.thestyleofme.comparison.common.domain;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/11 15:18
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobEnv {

    private String sourcePrestoCatalog;
    private String targetPrestoCatalog;

    private String sourceDatasourceCode;
    private String targetDatasourceCode;
    private String sourceSchema;
    private String targetSchema;
    private String sourceTable;
    private String targetTable;

    private String sourcePk;
    private String targetPk;
    private String sourceIndex;
    private String targetIndex;
    private List<Map<String,Object>> colMapping;
}
