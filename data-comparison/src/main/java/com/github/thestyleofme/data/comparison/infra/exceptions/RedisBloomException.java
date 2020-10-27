package com.github.thestyleofme.data.comparison.infra.exceptions;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 15:18
 * @since 1.0.0
 */
public class RedisBloomException extends RuntimeException {

    private static final long serialVersionUID = -4296115342868899859L;

    public RedisBloomException(String message) {
        super(message);
    }

    public RedisBloomException(String message, Object... params) {
        super(String.format(message, params));
    }

    public RedisBloomException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedisBloomException(Throwable cause) {
        super(cause);
    }
}
