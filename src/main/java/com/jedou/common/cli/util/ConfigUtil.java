package com.jedou.common.cli.util;

import java.io.FileReader;
import java.util.Properties;

import com.jedou.common.cli.logger.Logger;

public class ConfigUtil {

    private final static String configFileName = "main.conf";
    private static Properties properties = new Properties();

    public static synchronized void initProperties() throws Exception {
        Logger.info("加载运行时配置...");
        String absolutePath = ConfigUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String filePath = absolutePath.substring(0, absolutePath.lastIndexOf('/'))+'/'+configFileName;
        Logger.info("absolutePath: |%s", absolutePath);
        Logger.info("use config file: |%s", filePath);
        properties.load(new FileReader(filePath));
        Logger.info("加载完成，配置：|%s", properties);
    }

    public static String getProperty(String key) {
        return (String) properties.get(key);
    }

    public static String getProperty(String key, String defaultValue) {
        String value = (String) properties.get(key);
        if (value == null) value = defaultValue;
        return value;
    }

    public static Integer getInteger(String key) {
        String str = getProperty(key);
        return str == null ? null : Integer.parseInt(str);
    }

    public static Integer getInteger(String key, int defaultValue) {
        Integer value = getInteger(key);
        if (value == null) value = defaultValue;
        return value;
    }

    public static void main(String[] args) throws Exception {
        initProperties();
    }
}
