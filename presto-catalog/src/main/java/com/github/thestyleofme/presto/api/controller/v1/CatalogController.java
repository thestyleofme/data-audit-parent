package com.github.thestyleofme.presto.api.controller.v1;

import javax.validation.Valid;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.github.thestyleofme.presto.api.dto.CatalogDTO;
import com.github.thestyleofme.presto.app.service.CatalogService;
import com.github.thestyleofme.presto.domain.entity.Catalog;
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
 * @author isaac 2020/11/09 14:34
 * @since 1.0.0
 */
@RestController("prestoCatalogController.v1")
@RequestMapping("/v1/{organizationId}/presto-catalog")
@Slf4j
public class CatalogController {

    private final CatalogService catalogService;

    public CatalogController(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @ApiOperation(value = "查询catalog")
    @GetMapping
    public ResponseEntity<IPage<CatalogDTO>> list(@PathVariable(name = "organizationId") Long tenantId,
                                                  Page<Catalog> page,
                                                  CatalogDTO catalogDTO) {
        catalogDTO.setTenantId(tenantId);
        page.addOrder(OrderItem.desc(Catalog.FIELD_ID));
        return ResponseEntity.ok(catalogService.list(page, catalogDTO));
    }

    @ApiOperation(value = "创建或更新catalog")
    @PostMapping
    public ResponseEntity<CatalogDTO> save(@PathVariable(name = "organizationId") Long tenantId,
                                           @Valid @RequestBody CatalogDTO catalogDTO) {
        catalogDTO.setTenantId(tenantId);
        return ResponseEntity.ok(catalogService.save(catalogDTO));
    }

    @ApiOperation(value = "删除catalog")
    @DeleteMapping("/{clusterCode}/{catalogName}")
    public ResponseEntity<Void> delete(@PathVariable(name = "organizationId") Long tenantId,
                                       @PathVariable String clusterCode,
                                       @PathVariable String catalogName) {
        catalogService.delete(tenantId, clusterCode, catalogName);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

}
