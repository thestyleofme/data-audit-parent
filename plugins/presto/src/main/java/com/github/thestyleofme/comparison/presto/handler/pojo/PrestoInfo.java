package com.github.thestyleofme.comparison.presto.handler.pojo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.SelectTableInfo;
import com.github.thestyleofme.comparison.common.domain.entity.DataInfo;
import lombok.*;

/**
 * @author hsq
 * @date 2020-11-17 9:38
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PrestoInfo implements DataInfo {

    private String dataSourceCode;
    private String clusterCode;
    private String coordinatorUrl;
    private String username;


    private SelectTableInfo source;
    private SelectTableInfo target;
    private List<ColMapping> indexMapping;
    private List<ColMapping> colMapping;

    // presto database.catalog.tableName 表名

    private String sourceTableName;
    private String targetTableName;

}
