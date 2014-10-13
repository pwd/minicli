package com.jedou.common.cli.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by tiankai on 14-8-18.
 */
public class ClassUtil {
    public static <T> T create(final String className)
            throws ClassNotFoundException, InvocationTargetException,
            NoSuchMethodException, InstantiationException, IllegalAccessException {
        final Class<?> clazz = Class.forName(className);
        return (T) create(clazz);
    }
    public static <T> T create(final Class<T> classToCreate)
            throws NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        final Constructor<T> constructor;
            constructor = classToCreate.getDeclaredConstructor();
            final T result = constructor.newInstance();
            return result;
    }
    public static <T> T create(final String className, Class<T> subClass)
            throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException,
            InstantiationException, IllegalAccessException {
        final Class<T> clazz = (Class<T>) Class.forName(className).asSubclass(subClass);
        return create(clazz);
    }
}
