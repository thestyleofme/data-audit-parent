package com.github.thestyleofme.comparison.common.infra.handler.deploy.excel;

import java.util.List;

import com.alibaba.excel.EasyExcelFactory;
import com.alibaba.excel.ExcelReader;
import com.alibaba.excel.read.metadata.ReadSheet;
import com.alibaba.excel.util.StringUtils;
import com.github.thestyleofme.comparison.common.app.service.deploy.BaseDeployHandler;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.DeployInfo;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.DeployType;
import com.github.thestyleofme.comparison.common.infra.constants.CommonConstant;
import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.excel.InsertExcelListener;
import com.github.thestyleofme.comparison.common.infra.excel.UpdateExcelListener;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.common.infra.utils.ExcelUtil;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/20 14:09
 * @since 1.0.0
 */
@DeployType(CommonConstant.Deploy.EXCEL)
@Component
@Slf4j
public class ExcelDeployHandler implements BaseDeployHandler {

    private final DriverSessionService driverSessionService;

    public ExcelDeployHandler(DriverSessionService driverSessionService) {
        this.driverSessionService = driverSessionService;
    }

    @Override
    public void handle(ComparisonJob comparisonJob, DeployInfo deployInfo) {
        if (StringUtils.isEmpty(deployInfo.getTargetDataSourceCode())) {
            throw new HandlerException(ErrorCode.DEPLOY_EXCEL_DATASOURCE_NOT_FOUND);
        }
        String excelPath = ExcelUtil.getExcelPath(comparisonJob);
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(comparisonJob);
        List<List<String>> targetExcelHeader = ExcelUtil.getTargetExcelHeader(colMappingList);
        ExcelReader excelReader = null;
        try {
            excelReader = EasyExcelFactory.read(excelPath).build();
            // A有B无
            ReadSheet readSheet1 =
                    EasyExcelFactory.readSheet(0)
                            .head(ExcelUtil.getSourceExcelHeader(comparisonJob))
                            .registerReadListener(new InsertExcelListener<List<Object>>(
                                    comparisonJob, deployInfo.getTargetDataSourceCode(), targetExcelHeader,
                                    driverSessionService))
                            .build();
            // AB主键或唯一索引相同 覆盖
            ReadSheet readSheet2 =
                    EasyExcelFactory.readSheet(2)
                            .head(ExcelUtil.getSourceToTargetHeader(comparisonJob))
                            .registerReadListener(new UpdateExcelListener<List<Object>>(
                                    comparisonJob, deployInfo.getTargetDataSourceCode(), colMappingList,
                                    driverSessionService))
                            .build();
            excelReader.read(readSheet1, readSheet2);
        } finally {
            if (excelReader != null) {
                excelReader.finish();
            }
        }
    }
}
