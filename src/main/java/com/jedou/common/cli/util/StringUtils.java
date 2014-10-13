package com.jedou.common.cli.util;

/**
 * Created by tiankai on 14-8-15.
 */
public class StringUtils {
    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }
    public static boolean isNotEmpty(String str) {
        return str != null && str.length() > 0;
    }
}
