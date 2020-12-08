package com.github.thestyleofme.comparison.phoenix.constant;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/24 15:46
 * @since 1.0.0
 */
public enum RowTypeEnum {

    /**
     * A有B无
     */
    INSERT(0),
    /**
     * A无B有
     */
    DELETED(1),
    /**
     * AB主键或唯一索引相同
     */
    UPDATED(2),
    /**
     * 相同数据
     */
    SAME(3),
    ;

    private final int rawType;

    RowTypeEnum(int rawType) {
        this.rawType = rawType;
    }

    public int getRawType() {
        return rawType;
    }
}
