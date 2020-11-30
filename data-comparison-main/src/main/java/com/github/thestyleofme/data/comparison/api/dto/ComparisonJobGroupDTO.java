package com.github.thestyleofme.data.comparison.api.dto;

import java.time.LocalDateTime;
import javax.validation.constraints.NotBlank;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
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
@ApiModel("数据稽核任务组")
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ComparisonJobGroupDTO {

    public static final String FIELD_ID = "group_id";

    @TableId(type = IdType.AUTO)
    private Long groupId;
    @NotBlank
    @ApiModelProperty(value = "数据稽核任务组名称，英文下划线")
    private String groupCode;
    private String groupDesc;
    @NotBlank
    @ApiModelProperty(value = "源数据源")
    private String sourceDatasourceCode;
    @NotBlank
    @ApiModelProperty(value = "目标数据源")
    private String targetDatasourceCode;
    @NotBlank
    @ApiModelProperty(value = "源库")
    private String sourceSchema;
    @ApiModelProperty(value = "库下所有来源表的过滤条件")
    private String sourceWhere;
    @NotBlank
    @ApiModelProperty(value = "目标库")
    private String targetSchema;
    @ApiModelProperty(value = "库下所有目标表的过滤条件")
    private String targetWhere;

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
