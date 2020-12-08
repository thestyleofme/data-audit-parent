package com.github.thestyleofme.comparison.csv.pojo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.thestyleofme.comparison.common.domain.entity.Reader;
import lombok.*;

/**
 * <p></p>
 *
 * @author hsq 2020/12/02 17:49
 * @since 1.0.0
 */
@Data
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataxCsvReader extends Reader {

    public static final String DATAX_READER_NAME = "txtfilereader";

    private Parameter parameter;

    public DataxCsvReader() {
        this.setName(DATAX_READER_NAME);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Parameter {
        private String[] path;
        private String encoding;
        private List<Column> column;
        private String fieldDelimiter;
        @Builder.Default
        private boolean skipHeader = true;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Column {
        private int index;
        @Builder.Default
        private String type = "string";
    }
}
