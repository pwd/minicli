package com.jedou.common.cli.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.*;

/**
 * Created by tiankai on 14-8-15.
 */
public class NakedBeanUtil {
    public static class NakedBeanProperty {
        public Field field;
        public String name;
        public Object value;
        public Object[] array;

        public void toMap(Map m, boolean includingNull) {
            if (value != null || includingNull) {
                if (NakedBeanUtil.isSimpleType(field))
                    m.put(name, value);
                else if (NakedBeanUtil.isArrayType(field)) {
                    //					List li = new ArrayList();
                    //TODO: ...
                }
                else if (NakedBeanUtil.isListOrSetType(field)) {
                }
                else {
                    Map mm = NakedBeanUtil.toMap(value, includingNull);
                    m.put(name, mm);
                }
            }
        }
        public void toBean(Object o) throws IllegalArgumentException, IllegalAccessException {
            if (value != null) {
                if (NakedBeanUtil.isSimpleType(field))
                    field.set(o, NakedBeanUtil.parseValue(field.getType(), value));
                else if (NakedBeanUtil.isArrayType(field)) {
                }
                else if (NakedBeanUtil.isListOrSetType(field)) {
                }
                else {
                    Object oo = NakedBeanUtil.toBean((Map) value, field.getType());
                    field.set(o, oo);
                }
            }
        }
    }
    /**
     * 通过转换得到的数字类型值可能与原类中字段声明的类型不同，这里使用字段本身的类型尝试解析值类型以相匹配。
     */
    public static Object parseValue(Class clazz, Object rawValue) {
        if (rawValue == null) return null;

        Object value = null;
        if (clazz.equals(Integer.TYPE) || clazz.equals(Integer.class))
            value = ((Number) rawValue).intValue();
        else if (clazz.equals(Long.TYPE) || clazz.equals(Long.class))
            value = ((Number) rawValue).longValue();
        else if (clazz.equals(Float.TYPE) || clazz.equals(Float.class))
            value = ((Number) rawValue).floatValue();
        else if (clazz.equals(Double.TYPE) || clazz.equals(Double.class))
            value = ((Number) rawValue).doubleValue();
        else
            value = rawValue;

        return value;
    }

    public static Map toMap(Object bean) {
        return toMap(bean, true);
    }
    public static Map toMap(Object bean, boolean includingNull) {
        if (bean == null) return null;

        Map m = new HashMap();
        for (NakedBeanProperty nbp : getNakedBeanProperties(bean.getClass(), null, bean)) {
            nbp.toMap(m, includingNull);
        }
        return m;
    }

    @SuppressWarnings("unchecked")
    public static <T> T toBean(Map m, String className) {
        try {
            return (T) toBean(m, Class.forName(className));
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(String.format("将Map数据转换为对象 %s 出错",  className), ex);
        }
    }
    public static <T> T toBean(Map m, Class<T> clazz) {
        if (Map.class.equals(clazz)) return (T) m;
        T o = newInstance(clazz);
        for (NakedBeanProperty nbp : getNakedBeanProperties(clazz, m, null)) {
            try {
                nbp.toBean(o);
            } catch (Exception ex) {
                throw new RuntimeException(String.format("将Map数据转换为对象 %s 出错",  clazz == null ? null : clazz.getName()), ex);
            }
        }
        return o;
    }

    protected static <T> T newInstance(Class<T> clazz) {
        T o = null;
        try {
            o = clazz.newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(String.format("初始化对象 %s 出错",  clazz == null ? null : clazz.getName()), ex);
        }
        return o;
    }
    protected static List<NakedBeanProperty> getNakedBeanProperties(Class clazz, Map m, Object o) {
        List<NakedBeanProperty> rt = new ArrayList<NakedBeanProperty>();
        Field[] fs = clazz.getFields();
        try {
            for (Field f : fs) {
                if (isNakedProperty(f)) {
                    NakedBeanProperty nbp = new NakedBeanProperty();
                    nbp.field = f;
                    nbp.name = f.getName();
                    if (isArrayType(f)) {
                        //TODO:
                    }
                    else
                        nbp.value = m == null ? f.get(o) : m.get(f.getName());
                    rt.add(nbp);
                }
            }
        }
        catch (Exception ex) {
            throw new RuntimeException(String.format("初始化对象 %s 出错", clazz == null ? null : clazz.getName()), ex);
        }
        return rt;
    }

    protected static boolean isNakedProperty(Field f) {
        boolean rt = Modifier.isPublic(f.getModifiers())
                && (!Modifier.isStatic(f.getModifiers()))
                && (!Modifier.isFinal(f.getModifiers()))
                && (!Modifier.isAbstract(f.getModifiers()))
                ;
        return rt;
    }

    protected static boolean isSimpleType(Field f) {
        boolean rt = f.getType().isPrimitive();
        rt = rt
                ||f.getType().equals(String.class)
                || f.getType().equals(Integer.class)
                || f.getType().equals(Long.class)
                || f.getType().equals(Float.class)
                || f.getType().equals(Double.class)
                || f.getType().equals(Boolean.class)
                || f.getType().equals(BigDecimal.class)
                || f.getType().equals(Date.class)
        ;
        return rt;
    }

    protected static boolean isArrayType(Field f) {
        return f.getType().isArray();
    }

    protected static boolean isListOrSetType(Field f) {
        boolean rt = f.getType().equals(List.class)
                || f.getType().equals(Set.class)
                ;
        return rt;
    }

    protected static boolean isMapType(Field f) {
        return f.getType().equals(Map.class);
    }
}
