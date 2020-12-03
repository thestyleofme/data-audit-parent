package com.github.thestyleofme.presto.infra.exceptions;

import com.github.thestyleofme.comparison.common.infra.exceptions.BaseException;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/10 16:03
 * @since 1.0.0
 */
public class CatalogException extends BaseException {

    private static final long serialVersionUID = -733493082050898428L;

    public CatalogException(String message) {
        super(message);
    }

    public CatalogException(String message, Object... params) {
        super(message, params);
    }

    public CatalogException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogException(Throwable cause) {
        super(cause);
    }
}
