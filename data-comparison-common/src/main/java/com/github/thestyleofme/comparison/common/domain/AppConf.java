package com.github.thestyleofme.comparison.common.domain;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/11 13:57
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AppConf {

    private Map<String, Object> env;
    private Map<String, Map<String, Object>> source;
    private Map<String, Map<String, Object>> transform;
    private Map<String, Map<String, Object>> sink;

}
