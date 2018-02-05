package com.ociweb.pronghorn.structure.processor.property;

import com.ociweb.pronghorn.structure.annotations.ProngStruct;

import javax.lang.model.element.Element;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;

public enum PropertyStorage {
    Primitive,
    Boxed,
    String,
    Structure,
    Array;

    TypeKind getComponentKind(TypeMirror mirror) {
        switch (this) {
            case Boxed:
                return PropertyStorage.getBoxedPrimative(mirror);
            case Array:
                // TODO: Array
            case Primitive:
            case String:
            case Structure:
        }
        return mirror.getKind();
    }

    public static String defaultValuePrimative(TypeMirror mirror) {
        if (mirror.getKind() == TypeKind.DECLARED) {
            return defaultValuePrimative(getBoxedPrimative(mirror));
        }
        return defaultValuePrimative(mirror.getKind());

    }

    static TypeKind getBoxedPrimative(TypeMirror mirror) {
        switch(mirror.toString()) {
            case "java.lang.Boolean":
                return TypeKind.BOOLEAN;
            case "java.lang.Byte":
                return TypeKind.BYTE;
            case "java.lang.Character":
                return TypeKind.CHAR;
            case "java.lang.Double":
                return TypeKind.DOUBLE;
            case "java.lang.Float":
                return TypeKind.FLOAT;
            case "java.lang.Integer":
                return TypeKind.INT;
            case "java.lang.Long":
                return TypeKind.LONG;
            case "java.lang.Short":
                return TypeKind.SHORT;
        }
        return null;
    }

    static String defaultValuePrimative(TypeKind typeKind) {
        switch (typeKind) {
            case BOOLEAN:
                return "false";
            case BYTE:
            case SHORT:
            case INT:
                return "0";
            case LONG:
                return "0L";
            case CHAR:
                return "'\\0'";
            case FLOAT:
                return "0.0f";
            case DOUBLE:
                return "0.0";
        }
        return "null"; // DECLARED
    }

    public static PropertyStorage get(Types types, TypeMirror mirror) {
        if (mirror != null) {
            if (isProngPrimative(mirror)) {
                return PropertyStorage.Primitive;
            }
            else if (isProngBoxed(mirror)) {
                return PropertyStorage.Boxed;
            }
            else if (isProngString(mirror)) {
                return PropertyStorage.String;
            }
            else if (isProngStruct(types, mirror)) {
                return PropertyStorage.Structure;
            }
            else if (isProngArray(types, mirror)) {
                return PropertyStorage.Array;
            }
        }
        return null;
    }

    private static boolean isProngPrimative(TypeMirror mirror) {
        TypeKind kind = mirror.getKind();
        return kind.isPrimitive();
    }

    private static boolean isProngBoxed(TypeMirror mirror) {
        return mirror.getKind() == TypeKind.DECLARED && getBoxedPrimative(mirror) != null;
    }

    private static boolean isProngString(TypeMirror mirror) {
        TypeKind kind = mirror.getKind();
        if (kind == TypeKind.DECLARED) {
            if (mirror.toString().equals("java.lang.CharSequence")) {
                return  true;
            }
        }
        return false;
    }

    private static boolean isProngStruct(Types types, TypeMirror mirror) {
        TypeKind kind = mirror.getKind();
        if (kind == TypeKind.DECLARED) {
            Element element = types.asElement(mirror);
            if (element != null) {
                ProngStruct annotation = element.getAnnotation(ProngStruct.class);
                if (annotation != null) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isProngArray(Types types, TypeMirror mirror) {
        TypeKind kind = mirror.getKind();
        if (kind == TypeKind.ARRAY) {
            TypeMirror subType = ((ArrayType) mirror).getComponentType();
            if (subType != null) {
                PropertyStorage subStorage = get(types, subType);
                return (subStorage != null);
            }
        }
        return false;
    }
}
