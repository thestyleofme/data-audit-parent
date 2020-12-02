package com.github.thestyleofme.comparison.common.infra.exceptions;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 15:18
 * @since 1.0.0
 */
public class HandlerException extends RuntimeException {

    private static final long serialVersionUID = 831033366052355257L;

    private final String code;
    private final transient Object[] params;

    public String getCode() {
        return code;
    }

    public Object[] getParams() {
        return params;
    }

    public HandlerException(String message) {
        super(message);
        this.code = message;
        this.params = new Object[0];
    }

    public HandlerException(String message, Object... params) {
        super(message);
        this.code = message;
        this.params = params;
    }

    public HandlerException(String message, Throwable cause) {
        super(message, cause);
        this.code = message;
        this.params = new Object[0];
    }

    public HandlerException(Throwable cause) {
        super(cause);
        this.code = cause.getMessage();
        this.params = new Object[0];
    }
}
