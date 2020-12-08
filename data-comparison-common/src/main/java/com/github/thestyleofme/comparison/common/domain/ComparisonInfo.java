package com.github.thestyleofme.comparison.common.domain;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.thestyleofme.comparison.common.domain.entity.DataInfo;
import lombok.*;

/**
 * <p>比较信息Bean</p>
 *
 * @author hsq 2020/12/04 11:04
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ComparisonInfo implements DataInfo {
    private SelectTableInfo source;
    private SelectTableInfo target;
    private List<ColMapping> indexMapping;
    private List<ColMapping> colMapping;

    // 完整表名，如presto： database.catalog.tableName ，mysql：database.tableName

    private String sourceTableName;
    private String targetTableName;
}
