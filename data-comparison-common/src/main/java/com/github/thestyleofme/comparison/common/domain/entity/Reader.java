package com.github.thestyleofme.comparison.common.domain.entity;

/**
 * <p>Reader 基础类</p>
 *
 * @author hsq 2020/12/03 14:19
 * @since 1.0.0
 */
public abstract class Reader {

    /**
     * datax 插件名称
     */
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
