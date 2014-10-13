package com.jedou.common.cli.logger;

import java.util.Date;

/**
 * Created by tiankai on 14-8-15.
 */
public class Logger {
    public static LogLevel DefaultLogLevel = LogLevel.info;
    private static ThreadLocal<Date> current = new ThreadLocal<Date>();
    private static String getPrefix(LogLevel level) {
        if (current.get() == null) current.set(new Date());
        current.get().setTime(System.currentTimeMillis());
        return "["+current.get()+"]["+level.name()+"] ";
    }
    public static boolean isEnable(LogLevel level) {
        return DefaultLogLevel.getLevel() <= level.getLevel();
    }
    public static void debug(String msgPattern, Object...msgParam) {
        Log(LogLevel.debug, msgPattern, msgParam);
    }
    public static void info(String msgPattern, Object...msgParam) {
        Log(LogLevel.info, msgPattern, msgParam);
    }
    public static void warn(String msgPattern, Object...msgParam) {
        Log(LogLevel.warn, msgPattern, msgParam);
    }
    public static void error(String msgPattern, Object...msgParam) {
        Log(LogLevel.info, msgPattern, msgParam);
    }
    public static void Log(LogLevel level, String msgPattern, Object...msgParam) {
        if (isEnable(level))
            System.out.println(String.format(getPrefix(level)+msgPattern, msgParam));
    }
    public static void Exception(LogLevel level, Throwable exception) {
        if (isEnable(level)) {
            System.out.println(getPrefix(level));
            exception.printStackTrace();
        }
    }
}
