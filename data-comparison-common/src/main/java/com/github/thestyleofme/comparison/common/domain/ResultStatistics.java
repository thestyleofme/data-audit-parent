package com.github.thestyleofme.comparison.common.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * <p></p>
 *
 * @author hsq 2020/11/27 15:16
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResultStatistics {

    private Long insertCount;
    private String insertCountSql;
    private Long deleteCount;
    private String deleteCountSql;
    private Long updateCount;
    private String updateCountSql;
    private Long sameCount;
    private String sameCountSql;

}
