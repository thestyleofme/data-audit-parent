package com.github.thestyleofme.comparison.common.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.thestyleofme.comparison.common.infra.constants.DeployStrategyEnum;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * 数据补偿 -- API入参
 *
 * @author siqi.hou
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
    @ApiModelProperty(value = "补偿策略，默认REPLACE")
    @Builder.Default
    private String strategy = DeployStrategyEnum.REPLACE.name();

}
