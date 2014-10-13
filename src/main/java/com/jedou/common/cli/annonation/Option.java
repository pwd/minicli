package com.jedou.common.cli.annonation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by tiankai on 14-10-9.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Option {
    String value() default "";
    String longOpt() default "";
    String description() default "";
    boolean hasArg() default false;
    String argName() default "";
}
