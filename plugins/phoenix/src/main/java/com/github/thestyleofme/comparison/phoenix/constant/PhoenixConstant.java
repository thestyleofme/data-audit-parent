package com.github.thestyleofme.comparison.phoenix.constant;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/24 14:06
 * @since 1.0.0
 */
public class PhoenixConstant {

    private PhoenixConstant() {
    }

    public static final String CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS data_audit";

    public static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS data_audit.audit_result ( " +
            "pk BIGINT NOT NULL PRIMARY KEY,\n" +
            "\"-1\".row_type INTEGER,\n" +
            "\"-1\".job_name VARCHAR,\n" +
            "\"0\".event_time_ DATE) column_encoded_bytes=0";

    public static final String CREATE_SEQUENCE_SQL = "CREATE SEQUENCE IF NOT EXISTS data_audit.audit_result_sequence";

    public static final String UPSET_SQL_PREFIX = "UPSERT INTO data_audit.audit_result(" +
            "pk,\"0\".event_time_,\"-1\".row_type,\"-1\".job_name,";

    public static final String UPSET_SQL_VALUES = "VALUES(NEXT VALUE FOR data_audit.audit_result_sequence,CURRENT_DATE(),";

}
