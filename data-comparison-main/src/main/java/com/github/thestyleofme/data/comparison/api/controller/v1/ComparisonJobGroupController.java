package com.github.thestyleofme.data.comparison.api.controller.v1;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJobGroup;
import com.github.thestyleofme.data.comparison.api.dto.ComparisonJobGroupDTO;
import com.github.thestyleofme.data.comparison.app.service.ComparisonJobGroupService;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 11:08
 * @since 1.0.0
 */
@RestController("comparisonJobGroupController.v1")
@RequestMapping("/v1/{organizationId}/comparison-group")
@Slf4j
public class ComparisonJobGroupController {

    private final ComparisonJobGroupService comparisonJobGroupService;

    public ComparisonJobGroupController(ComparisonJobGroupService comparisonJobGroupService) {
        this.comparisonJobGroupService = comparisonJobGroupService;
    }


    @ApiOperation(value = "查询数据稽核任务组")
    @GetMapping
    public ResponseEntity<IPage<ComparisonJobGroupDTO>> list(@PathVariable(name = "organizationId") Long tenantId,
                                                             Page<ComparisonJobGroup> page,
                                                             ComparisonJobGroupDTO comparisonJobGroupDTO) {
        comparisonJobGroupDTO.setTenantId(tenantId);
        page.addOrder(OrderItem.desc(ComparisonJobGroup.FIELD_ID));
        return ResponseEntity.ok(comparisonJobGroupService.list(page, comparisonJobGroupDTO));
    }

    @ApiOperation(value = "保存数据稽核任务组")
    @PostMapping
    public ResponseEntity<ComparisonJobGroupDTO> save(@PathVariable(name = "organizationId") Long tenantId,
                                                      ComparisonJobGroupDTO comparisonJobGroupDTO) {
        comparisonJobGroupDTO.setTenantId(tenantId);
        return ResponseEntity.ok(comparisonJobGroupService.save(comparisonJobGroupDTO));
    }

    @ApiOperation(value = "删除数据稽核任务组")
    @DeleteMapping("/{groupId}")
    public ResponseEntity<Void> delete(@PathVariable(name = "organizationId") Long tenantId,
                                       @PathVariable Long groupId) {
        comparisonJobGroupService.removeById(groupId);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}
