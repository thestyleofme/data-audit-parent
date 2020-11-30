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

    private SelectTableInfo source;
    private SelectTableInfo target;
    /**
     * join on _a.id=_b.id
     * "joinMapping": [
     * {
     * "sourceCol": "id",
     * "targetCol": "id1"
     * }
     * ]
     */
    private List<Map<String, Object>> joinMapping;
    /**
     * "colMapping": [
     * {
     * "sourceCol": "id",
     * "targetCol": "id1",
     * "index": 0
     * },
     * {
     * "sourceCol": "name",
     * "targetCol": "name1",
     * "selected": true,
     * "index": 2
     * }
     * ]
     */
    private List<Map<String, Object>> colMapping;

}
