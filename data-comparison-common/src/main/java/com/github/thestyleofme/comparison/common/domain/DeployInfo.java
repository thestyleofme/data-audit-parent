package com.github.thestyleofme.comparison.common.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * 数据补偿 -- API入参
 *
 * @author siqi.hou@hand-china.com
 * @date 2020-11-19 11:19
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeployInfo {
    @ApiModelProperty(value = "租户ID")
    private Long tenantId;
    @ApiModelProperty(value = "数据稽核任务名称")
    private String jobCode;
    @ApiModelProperty(value = "数据稽核任务组")
    private String groupCode;
    @ApiModelProperty(value = "补偿类型")
    private String deployType;
    @ApiModelProperty(value = "补偿策略，默认replace")
    private String strategy;
}
