package com.github.thestyleofme.data.comparison.infra.utils;

import java.util.Objects;

import com.github.thestyleofme.plugin.core.infra.utils.ApplicationContextHelper;
import org.springframework.context.ApplicationContext;
import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/02 10:19
 * @since 1.0.0
 */
public class LocaleUtil {

    private LocaleUtil() {
    }

    private static MessageSource messageSource;

    static {
        ApplicationContext context = ApplicationContextHelper.getContext();
        if (Objects.nonNull(context)) {
            messageSource = context.getBean(MessageSource.class);
        }
    }

    /**
     * 获取单个国际化翻译值
     */
    public static String getMessage(String msgKey, Object... params) {
        try {
            return messageSource.getMessage(msgKey, params, LocaleContextHolder.getLocale());
        } catch (Exception e) {
            return msgKey;
        }
    }

}
