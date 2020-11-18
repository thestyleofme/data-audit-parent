package com.github.thestyleofme.data.comparison.api.dto;

import java.time.LocalDateTime;
import javax.validation.constraints.NotBlank;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 11:27
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ApiModel("数据稽核任务")
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ComparisonJobDTO {

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
    @ApiModelProperty(value = "任务执行开始时间")
    private LocalDateTime startTime;
    @ApiModelProperty(value = "任务状态")
    private String status;
    @ApiModelProperty(value = "任务执行时长")
    private String executeTime;
    @ApiModelProperty(value = "错误信息")
    private String errorMsg;

    @ApiModelProperty(value = "租户ID")
    private Long tenantId;
    @ApiModelProperty(hidden = true)
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
