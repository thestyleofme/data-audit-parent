package com.github.thestyleofme.comparison.csv.pojo;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p></p>
 *
 * @author hsq 2020/12/02 14:39
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CsvInfo {
    private String path;
}
