package com.ociweb.pronghorn.ring.proxy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE) //for class to map to message template
public @interface ProngTemplateMessage {

	long templateId() default 0;
  
}
