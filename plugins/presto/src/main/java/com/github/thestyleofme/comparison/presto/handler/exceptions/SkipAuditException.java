package com.github.thestyleofme.comparison.presto.handler.exceptions;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/30 17:19
 * @since 1.0.0
 */
public class SkipAuditException extends RuntimeException {

    private static final long serialVersionUID = 3110824954598824548L;

    public SkipAuditException(String message) {
        super(message);
    }

    public SkipAuditException(String message, Object... params) {
        super(String.format(message, params));
    }

    public SkipAuditException(String message, Throwable cause) {
        super(message, cause);
    }

    public SkipAuditException(Throwable cause) {
        super(cause);
    }
}
