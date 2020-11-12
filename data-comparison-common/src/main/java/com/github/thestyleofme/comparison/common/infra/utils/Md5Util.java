package com.github.thestyleofme.comparison.common.infra.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/19 14:54
 * @since 1.0.0
 */
@Slf4j
public class Md5Util {

    private Md5Util() {
        throw new IllegalStateException();
    }

    /**
     * 对字符串md5加密(大写+数字)
     *
     * @param str 传入要加密的字符串
     * @return MD5加密后的字符串
     */
    public static String getUppercaseMd5(String str) {
        char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
        try {
            byte[] btInput = str.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char[] str2 = new char[j * 2];
            int k = 0;
            for (byte byte0 : md) {
                str2[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str2[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str2);
        } catch (NoSuchAlgorithmException e) {
            log.error("Md5 encryption failed", e);
            throw new Md5Exception(e);
        }
    }

    public static class Md5Exception extends RuntimeException {

        private static final long serialVersionUID = -7115787413701597965L;

        public Md5Exception(String message, Throwable cause) {
            super(message, cause);
        }

        public Md5Exception(Throwable cause) {
            super(cause);
        }

    }
}
