package com.github.thestyleofme.data.comparison.infra.handler.comparators;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;

import com.github.thestyleofme.data.comparison.domain.entity.ColMapping;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 17:55
 * @since 1.0.0
 */
public class ColComparator implements Comparator<ColMapping>, Serializable {

    private static final long serialVersionUID = -2839638224183281291L;

    public static final ColComparator INSTANCE = new ColComparator();

    @Override
    public int compare(ColMapping o1, ColMapping o2) {
        return o1.getIndex() - o2.getIndex();
    }

}
