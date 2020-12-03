package com.github.thestyleofme.comparison.common.infra.exceptions;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/03 17:14
 * @since 1.0.0
 */
public class BaseException extends RuntimeException{

    private static final long serialVersionUID = 5096921294142304021L;

    private final String code;
    private final transient Object[] params;

    public String getCode() {
        return code;
    }

    public Object[] getParams() {
        return params;
    }

    public BaseException(String message) {
        super(message);
        this.code = message;
        this.params = new Object[0];
    }

    public BaseException(String message, Object... params) {
        super(message);
        this.code = message;
        this.params = params;
    }

    public BaseException(String message, Throwable cause) {
        super(message, cause);
        this.code = message;
        this.params = new Object[0];
    }

    public BaseException(Throwable cause) {
        super(cause);
        this.code = cause.getMessage();
        this.params = new Object[0];
    }
}
