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

    /**
     * 全局参数 执行时可从env中获取
     * 优先级低 只有source/transform/sink里没有配置该参数时，才从env取
     */
    private Map<String, Object> env;
    private Map<String, Map<String, Object>> source;
    private Map<String, Map<String, Object>> transform;
    private Map<String, Map<String, Object>> sink;

}
