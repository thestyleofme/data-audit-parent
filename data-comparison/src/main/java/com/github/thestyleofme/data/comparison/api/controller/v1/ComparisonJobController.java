package com.github.thestyleofme.data.comparison.api.controller.v1;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.github.thestyleofme.data.comparison.api.dto.ComparisonJobDTO;
import com.github.thestyleofme.data.comparison.app.service.ComparisonJobService;
import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 11:08
 * @since 1.0.0
 */
@RestController("comparisonJobController.v1")
@RequestMapping("/v1/{organizationId}/comparison")
@Slf4j
public class ComparisonJobController {

    private final ComparisonJobService comparisonJobService;

    public ComparisonJobController(ComparisonJobService comparisonJobService) {
        this.comparisonJobService = comparisonJobService;
    }

    @ApiOperation(value = "查询数据稽核job")
    @GetMapping
    public ResponseEntity<IPage<ComparisonJobDTO>> list(@PathVariable(name = "organizationId") Long tenantId,
                                                        Page<ComparisonJob> page,
                                                        ComparisonJobDTO comparisonJobDTO) {
        comparisonJobDTO.setTenantId(tenantId);
        page.addOrder(OrderItem.desc(ComparisonJob.FIELD_ID));
        return ResponseEntity.ok(comparisonJobService.list(page, comparisonJobDTO));
    }

    @ApiOperation(value = "执行数据稽核任务")
    @GetMapping("/execute/{id}")
    public ResponseEntity<Void> execute(@PathVariable(name = "organizationId") Long tenantId,
                                        @PathVariable(name = "id") Long id) {
        comparisonJobService.execute(tenantId, id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}
