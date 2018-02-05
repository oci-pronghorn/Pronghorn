package com.ociweb.pronghorn.structure.processor;

import com.ociweb.pronghorn.structure.annotations.ProngProperty;
import com.ociweb.pronghorn.structure.processor.property.PropertyStorage;

import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.ReferenceType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.util.ArrayList;
import java.util.List;

class RecursionCheck {
    private final Types types;
    private final Messager messenger;
    private final List<String> names;
    private final List<TypeMirror> stack = new ArrayList<>();

    RecursionCheck(Types types, Messager messenger) {
        this.names = new ArrayList<>();
        this.types = types;
        this.messenger = messenger;
    }
/*
    private RecursionCheck(List<String> names, Types types, Messager messenger) {
        this.names = new ArrayList<>(names);
        this.types = types;
        this.messenger = messenger;
    }
*/
    boolean recursionCheck(TypeElement element) {
        TypeMirror mirror = element.asType();
        this.names.add(element.getSimpleName().toString());
        return checkStructure(mirror);
    }

    private void error(Element element) {
        StringBuilder str = new StringBuilder();
        for (String name : names) {
            str.append(name);
            str.append(".");
        }
        str.delete(str.length() - 1, str.length());
        messenger.printMessage(Diagnostic.Kind.ERROR,
                "Recursive ProgStructs detected: " + str.toString(),
                element);
    }

    private boolean checkStructure(TypeMirror mirror) {
        Element element = types.asElement(mirror);
        if (stack.contains(mirror)) {
            error(element);
            return false;
        }
        stack.add(mirror);
        for (Element child : element.getEnclosedElements()) {
            if (child instanceof ExecutableElement) {
                if (!checkProperty((ExecutableElement)child)) {
                    return false;
                }
            }
        }
        stack.remove(stack.size()-1);
        return true;
    }

    private boolean checkProperty(ExecutableElement methodElement) {
        boolean isGetter = ExecutiveElementParser.fetchAccessorStyle(methodElement) == MethodType.Getter;
        if (isGetter) {
            TypeMirror propertyType = ExecutiveElementParser.fetchTypeMirror(methodElement, MethodType.Getter);
            if (propertyType != null && propertyType instanceof ReferenceType) {
                ProngProperty annotation = methodElement.getAnnotation(ProngProperty.class);
                boolean isNotNullable = (annotation == null || !annotation.nullable());
                if (isNotNullable) {
                    String methodName = methodElement.getSimpleName().toString();
                    String partialName = methodName.substring(4);
                    String name = methodName.substring(3, 4).toLowerCase() + partialName;
                    return recurseInto(name, (ReferenceType) propertyType);
                }
            }
        }
        return true;
    }

    private boolean recurseInto(String name, ReferenceType mirror) {
        PropertyStorage storage = PropertyStorage.get(types, mirror);
        if (storage != null) {
            if (storage == PropertyStorage.Structure) {
                names.add(name + ":" + types.asElement(mirror).getSimpleName());
                return checkStructure(mirror);
            } else if (storage == PropertyStorage.Array) {
                // Since the array may be null or empty and not init with members - it cannot cause the recursion.
                // Its subcomponents will be verified independently.
                return true;
            }
        }
        return true;
    }
}
