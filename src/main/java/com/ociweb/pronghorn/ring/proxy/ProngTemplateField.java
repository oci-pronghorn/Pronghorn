package com.ociweb.pronghorn.ring.proxy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD) //for field to map to message template field
public @interface ProngTemplateField {

	long fieldId() default 0;
    int decimalPlaces() default 2;
}
