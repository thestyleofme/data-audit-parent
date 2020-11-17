package com.github.thestyleofme.comparison.common.domain.entity;

import java.time.LocalDateTime;
import javax.validation.constraints.NotBlank;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
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
@TableName(value = "xadt_comparison_job")
public class ComparisonJob {

    public static final String FIELD_ID = "job_id";

    @TableId(type = IdType.AUTO)
    private Long jobId;
    private String groupCode;
    @NotBlank
    @ApiModelProperty(value = "数据稽核任务名称，英文下划线")
    private String jobCode;
    private String jobDesc;
    @NotBlank
    @ApiModelProperty(value = "任务模式(OPTION:页面向导/IMPORT:脚本配置即自行编写配置文件或自行上传配置文件)")
    private String jobMode;
    @NotBlank
    @ApiModelProperty(value = "数据稽核任务json配置文件")
    private String appConf;

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
