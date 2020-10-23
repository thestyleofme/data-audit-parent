package com.github.thestyleofme.data.comparison.domain.entity;

import java.time.LocalDateTime;
import java.util.List;
import javax.validation.constraints.NotBlank;

import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.Version;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/7/22 11:14
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ApiModel("数据稽核任务")
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@TableName(value = "data_comparison_job")
public class ComparisonJob {

    public static final String FIELD_ID = "id";

    private Long id;
    @NotBlank
    private String jobName;
    @NotBlank
    private String comparisonType;
    @ApiModelProperty(value = "引擎，redis_bloom_filter/presto")
    private String engineType;
    private String sourceDatasourceCode;
    private String targetDatasourceCode;
    private String sourceSchema;
    private String targetSchema;
    private String sourceTable;
    private String targetTable;
    @ApiModelProperty(value = "pk优先级比组合索引高")
    private String sourcePk;
    private String targetPk;
    private String sourceSql;
    private String targetSql;
    private String sourceFilePath;
    private String targetFilePath;
    @ApiModelProperty(value = "组合索引逗号分割")
    private String sourceIndex;
    private String targetIndex;
    @ApiModelProperty(value = "字段映射")
    private String colMapping;
    @ApiModelProperty(value = "额外配置")
    private String extConfig;

    @ApiModelProperty(value = "租户ID")
    private Long tenantId;
    @ApiModelProperty(hidden = true)
    @Version
    private Long objectVersionNumber;
    @ApiModelProperty(hidden = true)
    private LocalDateTime creationDate;
    @ApiModelProperty(hidden = true)
    private Long createdBy;
    @ApiModelProperty(hidden = true)
    private LocalDateTime lastUpdateDate;
    @ApiModelProperty(hidden = true)
    private Long lastUpdatedBy;

}
