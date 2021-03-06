package com.github.thestyleofme.comparison.phoenix.pojo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.thestyleofme.comparison.common.domain.entity.Reader;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * <p>
 * Phoenix Datax Reader
 * </p>
 *
 * @author isaac 2020/11/25 16:21
 * @since 1.0.0
 */
@Data
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataxPhoenixReader extends Reader {

    /**
     * name : hbase20xsqlreader
     * parameter : {"queryServerAddress":"http://172.23.16.57:8765","serialization":"PROTOBUF","querySql":["SELECT \"0\".\"id\",\"0\".\"name\",\"0\".\"sex\",\"0\".\"phone\",\"0\".\"address\",\"0\".\"education\",\"0\".\"state\" FROM data_audit.audit_result( \"0\".\"id\" VARCHAR, \"0\".\"name\" VARCHAR, \"0\".\"sex\" VARCHAR, \"0\".\"phone\" VARCHAR,\"0\".\"address\" VARCHAR,\"0\".\"education\" VARCHAR,\"0\".\"state\" VARCHAR) WHERE \"-1\".job_name='demo001' AND \"-1\".row_type=1"]}
     */

    public static final String DATAX_READER_NAME = "hbase20xsqlreader";

    public DataxPhoenixReader() {
        this.setName(DATAX_READER_NAME);
    }

    private Parameter parameter;

    @Data
    @Builder
    public static class Parameter {
        /**
         * queryServerAddress : http://172.23.16.57:8765
         * serialization : PROTOBUF
         * querySql : ["SELECT \"0\".\"id\",\"0\".\"name\",\"0\".\"sex\",\"0\".\"phone\",\"0\".\"address\",\"0\".\"education\",\"0\".\"state\" FROM data_audit.audit_result( \"0\".\"id\" VARCHAR, \"0\".\"name\" VARCHAR, \"0\".\"sex\" VARCHAR, \"0\".\"phone\" VARCHAR,\"0\".\"address\" VARCHAR,\"0\".\"education\" VARCHAR,\"0\".\"state\" VARCHAR) WHERE \"-1\".job_name='demo001' AND \"-1\".row_type=1"]
         */

        private String queryServerAddress;
        @Builder.Default
        private String serialization = "PROTOBUF";
        private List<String> querySql;
    }

}
