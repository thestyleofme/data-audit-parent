package com.github.thestyleofme.data.comparison.transform.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/18 9:30
 * @since 1.0.0
 */
@Data
@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public class BloomTransformInfo {

    private String type;
    private Double errorRate;
}
