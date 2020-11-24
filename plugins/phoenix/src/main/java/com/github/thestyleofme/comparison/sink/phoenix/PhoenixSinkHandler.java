package com.github.thestyleofme.comparison.sink.phoenix;

import java.util.Map;

import com.github.thestyleofme.comparison.common.app.service.sink.BaseSinkHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.SinkType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/23 17:39
 * @since 1.0.0
 */
@Component
@SinkType("PHOENIX")
@Slf4j
public class PhoenixSinkHandler implements BaseSinkHandler {

    @Override
    public void handle(ComparisonJob comparisonJob,
                       Map<String, Object> env,
                       Map<String, Object> sinkMap,
                       HandlerResult handlerResult) {
        // 创建数据源
        // 创表 创序列
        // A有B无 插入数据
        // AB冲突的按照更新策略插入
    }
}
