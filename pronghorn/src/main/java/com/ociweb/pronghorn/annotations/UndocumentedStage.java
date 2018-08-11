package com.ociweb.pronghorn.annotations;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)

@Target(ElementType.TYPE)

public @interface UndocumentedStage {

    public String value() default "UndocumentedStage";

}