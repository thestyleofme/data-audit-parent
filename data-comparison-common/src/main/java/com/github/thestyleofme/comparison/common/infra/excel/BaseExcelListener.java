package com.github.thestyleofme.comparison.common.infra.excel;

import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/18 16:11
 * @since 1.0.0
 */
public abstract class BaseExcelListener<T> extends AnalysisEventListener<T> {

    /**
     * 在添加到List集合之前(数据过滤，格式转换等)
     *
     * @param object 一行数据
     * @return boolean
     */
    abstract boolean addListBefore(T object);

    /**
     * 在添加到List集合之后(添加数据库，缓存等)
     *
     * @param object 一行数据
     */
    abstract void doListAfter(T object);

    /**
     * 最后的操作
     *
     * @param analysisContext AnalysisContext
     */
    abstract void doAfterAll(AnalysisContext analysisContext);

    /**
     * 执行类
     *
     * @param o               一行数据
     * @param analysisContext AnalysisContext
     */
    @SuppressWarnings("unchecked")
    @Override
    public void invoke(Object o, AnalysisContext analysisContext) {
        T t = (T) o;
        if (addListBefore(t)) {
            doListAfter(t);
        }
    }

    /**
     * 最后，做一些资源销毁
     *
     * @param analysisContext AnalysisContext
     */
    @Override
    public void doAfterAllAnalysed(AnalysisContext analysisContext) {
        doAfterAll(analysisContext);
    }
}
