package com.github.thestyleofme.comparison.common.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>源端或目标端配置信息</p>
 *
 * @author hsq 2020/11/27 16:02
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SelectTableInfo {

    /**
     * 只有presto时取catalog，其余情况取dataSourceCode
     */
    private String catalog;
    private String dataSourceCode;
    private String schema;
    private String table;
    private String where;
    /**
     * group中的where，所有表都会基于此进行过滤，如tenantId=0
     */
    private String globalWhere;
}
