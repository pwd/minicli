package com.jedou.common.cli.util;

import java.util.Collection;
import java.util.UUID;

/**
 * Created by tiankai on 14-8-15.
 */
public class Utils {
    public static String uuid() {
        return UUID.randomUUID().toString();
    }
    public static <E extends Collection> boolean isEmpty(E collection) {
        return collection == null || collection.isEmpty();
    }
    public static <E extends Collection> boolean isNotEmpty(E collection) {
        if (collection == null) return false;
        if (collection.isEmpty()) return false;
        return true;
    }
    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }
}
