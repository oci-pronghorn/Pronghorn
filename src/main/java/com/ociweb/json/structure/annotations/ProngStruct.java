package com.ociweb.json.structure.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/*
The intention of ProngStruct is as follows:
- behave as much as it can as a value-type (properties are copied, not shared) with JSON conformance
- adds flyweight functionality for garbage free use
- auto-generate efficient and correct standard POJO type methods (hashCode, etc)
- auto-generate getters and setters to efficiently obey null rules
- the auto-generation happens at compile-time so abstract/intefrface super can be used immediately and
    IDE generated code is not modified/checked-in.

When an interface or abstract class is annotated with ProngStruct, a concrete
implementation is generated with the following methods.

Construction
- Empty Constructor with annotation to invoke abstract's super
- Copy Constructor with annotation to invoke abstract's super
- assignFrom (recycle/copy) will invoke super with same signature
- clear (recycle new) will invoke super with same signature

Structure
- isEquals
- hashCode
- toString
- toString(StringBuilder, int indent)
- Externalizable methods

Member Storage and methods for abstract properties that can be represented in JSON
Use ProngProperty to annotate special behavior on the abstract methods.
    T get<Member>();
    void set<Member>(T rhs);
    boolean is<Member>Null();

    If the getter is not declared, a public getter is not created
    If the setter is not declared, a public setter is not created
    If the type is nullable primitive, is<Member>Null must be declared.
    If the type is nullable-set structure, declaring is<Member>Null will recycle struct after set<Member>(null).
*/

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ProngStruct {
    String suffix() default "Struct";
}

