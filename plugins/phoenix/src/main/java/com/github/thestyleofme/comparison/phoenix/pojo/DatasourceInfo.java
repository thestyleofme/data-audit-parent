package com.github.thestyleofme.comparison.phoenix.pojo;

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
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatasourceInfo {

    public static final String FIELD_JDBC_URL = "jdbcUrl";
    public static final String DEFAULT_CLASS_NAME = "org.apache.phoenix.queryserver.client.Driver";
    /**
     * jdbc:phoenix:thin:url=http://hdspdev001.hand-china.com:8765;serialization=PROTOBUF
     */
    private String jdbcUrl;

}
