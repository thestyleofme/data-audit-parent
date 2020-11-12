package com.github.thestyleofme.comparison.common.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 17:39
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ColMapping {

    public static final String SOURCE = "source";
    public static final String TARGET = "target";

    private String sourceCol;
    private String targetCol;
    private Integer index;
}
