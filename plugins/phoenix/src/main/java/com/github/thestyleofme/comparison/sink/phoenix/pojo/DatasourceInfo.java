package com.github.thestyleofme.comparison.sink.phoenix.pojo;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/23 17:09
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatasourceInfo {

    public static final String FIELD_JDBC_URL = "jdbcUrl";

    private String jdbcUrl;
    @Builder.Default
    private String driverClassName = "org.apache.phoenix.queryserver.client.Driver";

}
