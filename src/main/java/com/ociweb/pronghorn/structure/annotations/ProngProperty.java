package com.ociweb.pronghorn.structure.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/*
    A property's getter and setter may be annotated to describe the property's behavior.

    T get<Member>();
    void set<Member>(T rhs);

    If the getter is marked as nullable, the constructor will not allocate the member.
    If the setter is marked as nullable, the setter will allow a null input.

    If a recursion compiler error happens when building a ProngStruct then one of the
    properties in the chain must be declared with a nullable getter, therefore an
    infinite recursive construction at runtime is avoided.

   If a nullable primitive type is used the is<Member>Null method must be declared.
   If is<Member>Null method is declared for a structure, then the structure is recycled, even from null.
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ProngProperty {
    boolean nullableDefault = false;
    boolean nullable() default nullableDefault; // for non-primitives
}

