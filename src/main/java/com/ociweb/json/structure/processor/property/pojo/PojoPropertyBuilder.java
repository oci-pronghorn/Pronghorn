package com.ociweb.json.structure.processor.property.pojo;

import com.ociweb.json.structure.annotations.ProngProperty;
import com.ociweb.json.structure.processor.MethodType;
import com.ociweb.json.structure.processor.property.PropertyBuilderBase;
import com.ociweb.json.structure.processor.property.PropertyStorage;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.TypeMirror;

public class PojoPropertyBuilder extends PropertyBuilderBase {
    private final String fieldName;
    private final String suffix;
    private TypeName implType;

    PojoPropertyBuilder(ProcessingEnvironment env, String name, String suffix, boolean mutable) {
        super(env, name, mutable);
        this.fieldName = "_ph" + name;
        this.suffix = suffix;
    }

    public String discovered(MethodType methodType, TypeMirror mirror, ProngProperty propAnnotation) {
        String result = super.discovered(methodType, mirror, propAnnotation);
        if (mirror != null && storage() != null) {
            this.implType = getImplTypeName(mirror, storage(), suffix);
        }
        return result;
    }

    private static TypeName getImplTypeName(TypeMirror mirror, PropertyStorage storage, String suffix) {
        TypeName implType = TypeName.get(mirror);
        switch (storage) {
            case Primitive:
            case Boxed:
                break;
            case String:
                implType = ClassName.get(String.class);
                break;
            case Structure:
                implType = ClassName.bestGuess(mirror.toString() + suffix);
                break;
            case Array:
                // TODO: Array
                break;
        }
        return implType;
    }

    @Override
    public String privateAccess() {
        return fieldName;
    }

    @Override
    public TypeName privateAccessType() {
        return implType;
    }

    @Override
    public String privateIsNullBackstoreAccess() {
        return fieldName + "Null";
    }

    @Override
    public void buildStorage(TypeSpec.Builder builder) {
        builder.addField(implType, fieldName, Modifier.PRIVATE);
        if (hasIsNullBackStore()) {
            builder.addField(boolean.class, privateIsNullBackstoreAccess(), Modifier.PRIVATE);
        }
    }

    // Accessors

    protected void implementGetter(MethodSpec.Builder getter) {
        getValue(getter);
    }

    protected void implementIsNull(MethodSpec.Builder isNull) {
        getIsNull(isNull);
    }

    // Init

    public void implementInit(MethodSpec.Builder method) {
        mutateValueInit(method, true);
    }

    public void implementClear(MethodSpec.Builder method) {
        mutateValueInit(method, false);
    }

    // Copy

    public void implementCopyInit(MethodSpec.Builder method, String rhs) {
        mutateValueCopy(method, rhs,true);
    }

    public void implementAssign(MethodSpec.Builder assignFrom, String rhs) {
        mutateValueCopy(assignFrom, rhs,false);
    }

    // Mutate

    protected void implementSetter(MethodSpec.Builder setter, String rhs) {
        mutateValueSet(setter, rhs);
    }

    protected void implementSetNull(MethodSpec.Builder isNull) {
        mutateValueSet(isNull,null );
    }

    protected void ensureStructAllocated(MethodSpec.Builder builder) {
        if (optional().isNullable()) {
            builder.beginControlFlow("if ($L != null)", fieldName);
            builder.addStatement("$L = new $T()", fieldName, implType);
            builder.endControlFlow();
        }
    }

    // Builders...

    private void getIsNull(MethodSpec.Builder method) {
        method.addStatement("return $L", nullCondition());
    }

    private void getValue(MethodSpec.Builder getter) {
        switch (storage()) {
            case Primitive:
                getter.addStatement("return $L", fieldName);
                break;
            case Boxed:
                getter.addStatement("return $L", fieldName);
                break;
            case String:
                getter.addStatement("return $L", fieldName);
                break;
            case Structure:
                if (hasIsNullBackStore()) {
                    getter.addStatement("return $L ? null : $L", privateIsNullBackstoreAccess(), fieldName);
                }
                else {
                    getter.addStatement("return $L", fieldName);
                }
                break;
            case Array:
                // TODO: Array
                break;
        }
    }

    private void mutateValueInit(MethodSpec.Builder method, boolean constructor) {
        boolean gotoNull = optional().getterNullable();
        boolean canBeNull = optional().isNullable();
        if (hasIsNullBackStore()) {
            method.addStatement("$L = $L", privateIsNullBackstoreAccess(), gotoNull);
        }
        switch (storage()) {
            case Primitive:
                method.addStatement("$L = $L", fieldName, PropertyStorage.defaultValuePrimative(implType()));
                break;
            case Boxed:
                if (gotoNull) {
                    method.addStatement("$L = null", fieldName);
                } else {
                    method.addStatement("$L = $L", fieldName, PropertyStorage.defaultValuePrimative(implType()));
                }
                break;
            case String:
                if (gotoNull) {
                    method.addStatement("$L = null", fieldName);
                } else {
                    method.addStatement("$L = \"\"", fieldName);
                }
                break;
            case Structure:
                if (gotoNull) {
                    if (constructor || !hasIsNullBackStore()) {
                        method.addStatement("$L = null", fieldName);
                    }
                    else { // clear with backstore - clear the struct if not currently null
                        method.beginControlFlow("if ($L != null)", fieldName);
                        method.addStatement("$L.clear()", fieldName);
                        method.endControlFlow();
                    }
                }
                else if (canBeNull) {
                    if (constructor) {
                        method.addStatement("$L = new $T()", fieldName, implType);
                    }
                    else {
                        method.beginControlFlow("if ($L != null)", fieldName);
                        method.addStatement("$L.clear()", fieldName);
                        method.nextControlFlow("else");
                        method.addStatement("$L = new $T()", fieldName, implType);
                        method.endControlFlow();
                    }
                }
                else {
                    if (constructor) {
                        method.addStatement("$L = new $T()", fieldName, implType);
                    } else {
                        method.addStatement("$L.clear()", fieldName);
                    }
                }
                break;
            case Array:
                // TODO: Array
                break;
        }
    }

    private void mutateValueSet(MethodSpec.Builder method, String rhs) {
        boolean toNull = rhs == null;
        switch (storage()) {
            case Primitive:
                if (hasIsNullBackStore()) {
                    method.addStatement("$L = $L", privateIsNullBackstoreAccess(), toNull);
                }
                method.addStatement("$L = $L", fieldName, toNull ? PropertyStorage.defaultValuePrimative(accessType()) : rhs);
                break;
            case Boxed:
                method.addStatement("$L = $L", fieldName, toNull ? "null" : rhs);
                break;
            case String:
                if (toNull) {
                    method.addStatement("$L = null", fieldName);
                }
                else {
                    // TODO: create a copy on write impl to avoid this new
                    method.addStatement("$L = $L.toString()", fieldName, rhs);
                }
                break;
            case Structure:
                if (toNull) {
                    if (hasIsNullBackStore()) {
                        method.addStatement("$L = true", privateIsNullBackstoreAccess());
                        method.beginControlFlow("if ($L != null)", fieldName);
                        method.addStatement("$L.clear()", fieldName);
                        method.endControlFlow();
                    }
                    else {
                        method.addStatement("$L = null", fieldName);
                    }
                }
                else {
                    if (optional().setterNullable()) {
                        method.beginControlFlow("if ($L == null)", rhs);
                        implementSetNull(method);
                        method.nextControlFlow("else");
                        if (hasIsNullBackStore()) {
                            method.addStatement("$L = false", privateIsNullBackstoreAccess());
                        }
                        method.beginControlFlow("if ($L == null)", fieldName);
                        method.addStatement("$L = new $T($L)", fieldName, implType, rhs);
                        method.nextControlFlow("else");
                        method.addStatement("$L.assignFrom($L)", fieldName, rhs);
                        method.endControlFlow();
                        method.endControlFlow();
                    }
                    else if (optional().getterNullable()) {
                        if (hasIsNullBackStore()) {
                            method.addStatement("$L = false", privateIsNullBackstoreAccess());
                        }
                        method.beginControlFlow("if ($L == null)", fieldName);
                        method.addStatement("$L = new $T($L)", fieldName, implType, rhs);
                        method.nextControlFlow("else");
                        method.addStatement("$L.assignFrom($L)", fieldName, rhs);
                        method.endControlFlow();
                    }
                    else {
                        method.addStatement("$L.assignFrom($L)", fieldName, rhs);
                    }
                }
                break;
            case Array:
                // TODO: Array
                break;
        }
    }

    private void mutateValueCopy(MethodSpec.Builder method, String rhs, boolean constructor) {
        String publicAccess = publicAccess();
        if (publicAccess == null) return;
        if (hasIsNullBackStore()) {
            method.addStatement("$L = $L.$L", privateIsNullBackstoreAccess(), rhs, publicIsNull());
        }
        switch (storage()) {
            case Primitive:
                method.addStatement("$L = $L.$L", fieldName, rhs, publicAccess);
                break;
            case Boxed:
                method.addStatement("$L = $L.$L", fieldName, rhs, publicAccess);
                break;
            case String:
                // TODO: create a copy on write impl to avoid this new
                method.addStatement("$L = $L.toString()", fieldName, rhs);
                break;
            case Structure:
                if (constructor) {
                    if (!optional().isNullable()) {
                        method.addStatement("$L = new $T($L.$L)", fieldName, implType, rhs, publicAccess);
                    } else {
                        if (hasIsNullBackStore()) {
                            method.beginControlFlow("if (!$L)", privateIsNullBackstoreAccess());
                            method.addStatement("$L = new $T($L.$L)", fieldName, implType, rhs, publicAccess);
                            method.endControlFlow();
                        }
                        else {
                            method.addStatement("$T temp$L = $L.$L", accessType(), name(), rhs, publicAccess);
                            method.beginControlFlow("if (temp$L != null)", name());
                            method.addStatement("$L = new $T(temp$L)", fieldName, implType, name());
                            method.endControlFlow();
                        }
                    }
                }
                else {
                    if (!optional().isNullable()) {
                        method.addStatement("$L.assignFrom($L.$L)", fieldName, rhs, publicAccess);
                    }
                    else {
                        if (hasIsNullBackStore()) {
                            method.beginControlFlow("if ($L)", privateIsNullBackstoreAccess());
                            method.beginControlFlow("if ($L != null)", fieldName);
                            method.addStatement("$L.clear()", fieldName);
                            method.endControlFlow();
                            method.nextControlFlow("else");
                            method.beginControlFlow("if ($L == null)", fieldName);
                            method.addStatement("$L = new $T($L.$L)", fieldName, implType, rhs, publicAccess);
                            method.nextControlFlow("else");
                            method.addStatement("$L.assignFrom($L.$L)", fieldName, rhs, publicAccess);
                            method.endControlFlow();
                            method.endControlFlow();
                        }
                        else {
                            method.addStatement("$T temp$L = $L.$L", accessType(), name(), rhs, publicAccess);
                            method.beginControlFlow("if (temp$L == null)", name());
                            method.addStatement("$L = null", fieldName);
                            method.nextControlFlow("else");
                            method.beginControlFlow("if ($L == null)", fieldName);
                            method.addStatement("$L = new $T(temp$L)", fieldName, implType, name());
                            method.nextControlFlow("else");
                            method.addStatement("$L.assignFrom(temp$L)", fieldName, name());
                            method.endControlFlow();
                            method.endControlFlow();
                        }
                    }
                }
                break;
            case Array:
                // TODO: Array
                break;
        }
    }
}
