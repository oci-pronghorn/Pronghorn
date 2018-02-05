package com.ociweb.pronghorn.structure.processor;

import com.ociweb.pronghorn.structure.annotations.ProngProperty;
import com.squareup.javapoet.MethodSpec;

import javax.lang.model.element.*;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import java.util.List;
import java.util.Set;

class MethodMatch {
    boolean isOverride;
    boolean canInvokeSuper;
    boolean canReturnSuper;
}

class ExecutiveElementParser {
    static MethodMatch matchMethod(Types types, Element baseClass, MethodSpec.Builder builder) {
        for (Element enclosed : baseClass.getEnclosedElements()) {
            if (enclosed.getKind() == ElementKind.METHOD || enclosed.getKind() == ElementKind.CONSTRUCTOR) {
                MethodSpec method = builder.build();
                MethodMatch match = matchMethod(types, (ExecutableElement)enclosed, method);
                if (match != null) {
                    return match;
                }
            }
        }
        if (baseClass instanceof TypeElement) {
            Element superClass = types.asElement(((TypeElement) baseClass).getSuperclass());
            if (superClass != null && !superClass.getSimpleName().toString().equals("Object")) {
                MethodMatch match = matchMethod(types, superClass, builder);
                if (match != null) {
                    return match;
                }
            }
        }
        return new MethodMatch();
    }

    private static MethodMatch matchMethod(Types types, ExecutableElement element, MethodSpec method) {
        boolean isConstructor = method.isConstructor();
        if (isConstructor && !element.getSimpleName().toString().equals("<init>")) {
            return null;
        }
        else  if (!element.getSimpleName().toString().equals(method.name)) {
            return null;
        }
        List<? extends VariableElement> methodParams = element.getParameters();
        if (method.parameters.size() != methodParams.size()) {
            return null;
        }
        Set<Modifier> modifiers = element.getModifiers();
        if (modifiers.contains(Modifier.STATIC)) {
            return null;
        }

        boolean isConcrete = !modifiers.contains(Modifier.ABSTRACT);
        boolean canAccess = !modifiers.contains(Modifier.PRIVATE);
        boolean canBeOverride = !modifiers.contains(Modifier.FINAL);

        MethodMatch match = new MethodMatch();
        match.isOverride = canBeOverride && !isConstructor;
        match.canReturnSuper = isConcrete && canAccess;
        match.canInvokeSuper = isConcrete && canAccess;
/*
        TODO: How do we bridge TypeMirrors With elements with MethodSpec's to do the following?
        TypeMirror retrn =
        TypeMirror[] params =

        if (!types.isSameType(element.getReturnType(), retrn)) {
            match.isOverride = false;
        }
        else if (!types.isAssignable(element.getReturnType(), retrn)) {
            match.canReturnSuper = false;
        }
        for (int i = 0; i < params.length; i++) {
            if (match.isOverride && !types.isSameType(params[i], methodParams.get(i).asType())) {
                match.isOverride = false;
            }
            if (match.canInvokeSuper && !types.isAssignable(params[i], methodParams.get(i).asType())) {
                match.canInvokeSuper = false;
            }
        }
        */
        return match;
    }

    static MethodType fetchAccessorStyle(ExecutableElement element) {
        boolean isAbstract = element.getModifiers().contains(Modifier.ABSTRACT);
        boolean isDecalaredAsProperty = element.getAnnotation(ProngProperty.class) != null;
        if (!isAbstract && !isDecalaredAsProperty) {
            return null;
        }
        String methodName = element.getSimpleName().toString();
        if (methodName.length() <= 3) {
            return null;
        }
        if (methodName.startsWith("get")) {
            return element.getParameters().size() == 0 ? MethodType.Getter : null;
        }
        else if (methodName.startsWith("set")) {
            return element.getParameters().size() == 1 ? MethodType.Setter : null;
        }
        else if (methodName.startsWith("is") && methodName.endsWith("Null")) {
            return element.getParameters().size() == 0 ? MethodType.IsNull : null;
        }
        return  null;
    }

    static TypeMirror fetchTypeMirror(ExecutableElement element, MethodType methodType) {
        switch (methodType) {
            case Getter:
                return element.getReturnType();
            case Setter:
                return element.getParameters().get(0).asType();
            case IsNull:
        }
        return null;
    }
}
