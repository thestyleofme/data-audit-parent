package com.github.thestyleofme.data.comparison.infra.context;

import java.util.HashMap;
import java.util.Map;

import com.github.thestyleofme.comparison.common.app.service.deploy.BaseDeployHandler;
import com.github.thestyleofme.comparison.common.app.service.sink.BaseSinkHandler;
import com.github.thestyleofme.comparison.common.app.service.sink.SinkHandlerProxy;
import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.TransformHandlerProxy;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 15:21
 * @since 1.0.0
 */
@Component
@Slf4j
public class JobHandlerContext {

    private static final Map<String, BaseTransformHandler> TRANSFORM_HANDLER_MAP = new HashMap<>();
    private static final Map<String, TransformHandlerProxy> TRANSFORM_HANDLER_PROXY_MAP = new HashMap<>();
    private static final Map<String, BaseSinkHandler> SINK_HANDLER_MAP = new HashMap<>();
    private static final Map<String, SinkHandlerProxy> SINK_HANDLER_PROXY_MAP = new HashMap<>();
    private static final Map<String, BaseDeployHandler> DEPLOY_HANDLER_MAP = new HashMap<>();

    //===============================================================================
    //  TransformType
    //===============================================================================

    public void register(String transformType, BaseTransformHandler baseTransformHandler) {
        if (TRANSFORM_HANDLER_MAP.containsKey(transformType)) {
            log.error("transformType {} exists", transformType);
            throw new HandlerException("error.comparison.job.handler.transformType.exist");
        }
        TRANSFORM_HANDLER_MAP.put(transformType, baseTransformHandler);
    }

    public void register(String transformType, TransformHandlerProxy baseTransformHandler) {
        if (TRANSFORM_HANDLER_PROXY_MAP.containsKey(transformType)) {
            log.error("TransformHandlerProxy {} exists", transformType);
            throw new HandlerException("error.comparison.job.handler.TransformHandlerProxy.exist");
        }
        TRANSFORM_HANDLER_PROXY_MAP.put(transformType, baseTransformHandler);
    }

    public BaseTransformHandler getTransformHandler(String transformType) {
        BaseTransformHandler baseTransformHandler = TRANSFORM_HANDLER_MAP.get(transformType.toUpperCase());
        if (baseTransformHandler == null) {
            throw new HandlerException("error.comparison.job.handler.transformType[%s].not.exist", transformType);
        }
        return baseTransformHandler;
    }

    public TransformHandlerProxy getTransformHandlerProxy(String transformType) {
        return TRANSFORM_HANDLER_PROXY_MAP.get(transformType.toUpperCase());
    }

    //===============================================================================
    //  SinkType
    //===============================================================================

    public void register(String sinkType, BaseSinkHandler baseSinkHandler) {
        if (SINK_HANDLER_MAP.containsKey(sinkType)) {
            log.error("sinkType {} exists", sinkType);
            throw new HandlerException("error.comparison.job.handler.sinkType.exist");
        }
        SINK_HANDLER_MAP.put(sinkType, baseSinkHandler);
    }

    public void register(String sinkType, SinkHandlerProxy sinkHandlerProxy) {
        if (SINK_HANDLER_PROXY_MAP.containsKey(sinkType)) {
            log.error("SinkHandlerProxy {} exists", sinkType);
            throw new HandlerException("error.comparison.job.handler.SinkHandlerProxy.exist");
        }
        SINK_HANDLER_PROXY_MAP.put(sinkType, sinkHandlerProxy);
    }

    public BaseSinkHandler getSinkHandler(String outputType) {
        BaseSinkHandler baseSinkHandler = SINK_HANDLER_MAP.get(outputType.toUpperCase());
        if (baseSinkHandler == null) {
            throw new HandlerException("error.comparison.job.handler.sinkType.not.exist");
        }
        return baseSinkHandler;
    }

    public SinkHandlerProxy getSinkHandlerProxy(String sinkType) {
        return SINK_HANDLER_PROXY_MAP.get(sinkType.toUpperCase());
    }

    //===============================================================================
    //  DeployType
    //===============================================================================

    public void register(String deployType, BaseDeployHandler deployHandler) {
        if (DEPLOY_HANDLER_MAP.containsKey(deployType)) {
            log.error("deployType {} exists", deployType);
            throw new HandlerException("hdsp.xadt.error.deploy.handler.deployType.exist");
        }
        DEPLOY_HANDLER_MAP.put(deployType, deployHandler);
    }

    public BaseDeployHandler getDeployHandler(String deployType) {
        BaseDeployHandler deployHandler = DEPLOY_HANDLER_MAP.get(deployType.toUpperCase());
        if (deployHandler == null) {
            throw new HandlerException("hdsp.xadt.error.deploy.handler.deployType.not_exist");
        }
        return deployHandler;
    }
}
