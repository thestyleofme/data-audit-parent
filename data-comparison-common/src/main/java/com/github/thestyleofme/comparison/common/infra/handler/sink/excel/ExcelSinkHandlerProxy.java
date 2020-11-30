package com.github.thestyleofme.comparison.common.infra.handler.sink.excel;

import java.util.Map;

import com.github.thestyleofme.comparison.common.app.service.sink.BaseSinkHandler;
import com.github.thestyleofme.comparison.common.app.service.sink.SinkHandlerProxy;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.SinkType;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.common.infra.utils.BeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/12 16:08
 * @since 1.0.0
 */
@Component
@SinkType("EXCEL")
@Slf4j
public class ExcelSinkHandlerProxy implements SinkHandlerProxy {

    @SuppressWarnings("unchecked")
    @Override
    public BaseSinkHandler proxy(BaseSinkHandler baseSinkHandler) {
        return CommonUtil.createProxy(
                baseSinkHandler.getClass().getClassLoader(),
                BaseSinkHandler.class,
                (proxy, method, args) -> {
                    try {
                        return method.invoke(baseSinkHandler, args);
                    } catch (Exception e) {
                        // 抛异常需要将文件删除
                        Map<String, Object> sinkMap = (Map<String, Object>) args[2];
                        ExcelInfo excelInfo = BeanUtils.map2Bean(sinkMap, ExcelInfo.class);
                        ComparisonJob comparisonJob = (ComparisonJob) args[0];
                        CommonUtil.deleteFile(comparisonJob, excelInfo.getOutputPath());
                        throw e;
                    }
                }
        );
    }
}
