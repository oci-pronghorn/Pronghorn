package com.ociweb.pronghorn.structure.processor.property;

import com.ociweb.pronghorn.structure.annotations.ProngProperty;
import com.ociweb.pronghorn.structure.processor.MethodType;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class PropertyBuilderBase {
    private final ProcessingEnvironment env;
    private final boolean mutable;
    private final String name;

    private PropertyStorage storage;
    private TypeMirror impl;
    private boolean hasIsNullBackStore;
    private TypeMirror getter;
    private TypeMirror setter;
    private boolean isXNull;

    private ProngPropertyOptional optional = ProngPropertyOptional.not;

    public PropertyBuilderBase(ProcessingEnvironment env, String name, boolean mutable) {
        this.env = env;
        this.mutable = mutable;
        this.name = name;
    }

    public String discovered(MethodType methodType, TypeMirror mirror, ProngProperty propAnnotation) {
        if (mirror != null) {
            if (this.impl == null) {
                this.impl = mirror;
            } else {
                if (env.getTypeUtils().isSubtype(mirror, this.impl)) {
                    this.impl = mirror;
                }
                else if (!env.getTypeUtils().isSubtype(this.impl, mirror)) {
                    return methodType.toString() + "<" + mirror + ">" + name + " is not superType of " + this.impl;
                }
            }
        }

        switch (methodType) {
            case Getter:
                getter = mirror;
                break;
            case Setter:
                setter = mirror;
                break;
            case IsNull:
                isXNull = true;
                break;
        }

        if (propAnnotation != null && propAnnotation.nullable() && methodType != MethodType.IsNull) {
            optional = optional.accumeOptional(true, methodType == MethodType.Getter);
        }
        if (this.impl != null && storage == null) {
            storage = PropertyStorage.get(env.getTypeUtils(), this.impl);
        }
        if (storage == PropertyStorage.Primitive && optional.isNullable()) {
            hasIsNullBackStore = true;
        }
        if (storage == PropertyStorage.Structure && isXNull) {
            hasIsNullBackStore = true;
        }
        return null;
    }

    private String truncate(Object obj) {
        if (obj == null) {
            return "_";
        }
        String str = obj.toString();
        int idx = str.lastIndexOf('.');
        if (idx == -1) {
            return str;
        }
        return str.substring(idx + 1);
    }

    @Override
    public String toString() {
        String type = truncate(setter) + (optional.setterNullable() ? "?" : "") + ":" + truncate(impl) + "->" + truncate(getter) + (optional.getterNullable() ? "?" : "");
        return name + "<" + type + ">";
    }

    protected TypeMirror implType() {
        return impl;
    }

    protected TypeMirror accessType() {
        return getter;
    }

    protected boolean hasIsNullBackStore() {
        return hasIsNullBackStore;
    }

    public String name() {
        return name;
    }

    public PropertyStorage storage() {
        return storage;
    }

    public ProngPropertyOptional optional() {
        return optional;
    }

    // Public Accessors

    protected String publicAccess() {
        return getter != null ? "get" + name + "()" : null;
    }

    protected String publicIsNull() {
        return "is" + name + "Null()";
    }

    // Private Accessors

    public String privateAccess() {
        return null;
    }

    public TypeName privateAccessType() {
        return null;
    }

    public String privateIsNullBackstoreAccess() {
        return null;
    }

    // Null validation

    protected String nullCondition() {
        if (optional().isNullable()) {
            if (hasIsNullBackStore()) {
                return privateIsNullBackstoreAccess();
            } else if (impl.getKind() == TypeKind.DECLARED) {
                return privateAccess() + " == null";
            }
        }
        return "false";
    }

    private void assertOnSet(MethodSpec.Builder method, String rhs) {
        if (!optional.setterNullable() && setter.getKind() == TypeKind.DECLARED) {
            method.addStatement("assert($L != null)", rhs);
        }
    }

    // Builders

    public void buildAccessors(TypeSpec.Builder builder) {
        buildStorage(builder);
        buildGetter(builder);
        buildSetter(builder);
        buildIsNull(builder);
        buildSetNull(builder);
    }

    public void buildAbstractAccessors(TypeSpec.Builder builder) {
        declareAbstractGetter(builder);
        buildSetter(builder);
        buildSetNull(builder);
    }

    public void buildStorage(TypeSpec.Builder builder) {
        // whatever the subclass needs to reference the data
    }

    public void declareAbstractGetter(TypeSpec.Builder builder) {
        if (getter != null) {
            MethodSpec.Builder method = MethodSpec.methodBuilder("get" + name)
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .returns(TypeName.get(getter));
            builder.addMethod(method.build());
        }

        if (optional.isNullable()) {
            MethodSpec.Builder isNull = MethodSpec.methodBuilder("is" + name + "Null")
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .returns(boolean.class);
            builder.addMethod(isNull.build());
        }
    }

    private void buildGetter(TypeSpec.Builder builder) {
        if (getter != null) {
            MethodSpec.Builder method = MethodSpec.methodBuilder("get" + name)
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(TypeName.get(getter));
            implementGetter(method);
            builder.addMethod(method.build());
        }
    }

    private void buildSetter(TypeSpec.Builder builder) {
        if (setter != null) {
            MethodSpec.Builder method = MethodSpec.methodBuilder("set" + name)
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(TypeName.get(setter), "in" + name);
            if (mutable) {
                assertOnSet(method, "in" + name);
                implementSetter(method, "in" + name);
            }
            else {
                method.addStatement("assert(false) : \"$L is immutable\"", name);
            }
            builder.addMethod(method.build());
        }
    }

    private void buildIsNull(TypeSpec.Builder builder) {
        if (optional.isNullable()) {
            MethodSpec.Builder isNull = MethodSpec.methodBuilder("is" + name + "Null")
                    .addModifiers(Modifier.PUBLIC)
                    .returns(boolean.class);
            if (hasIsNullBackStore()) {
                isNull.addAnnotation(Override.class);
            }
            implementIsNull(isNull);
            builder.addMethod(isNull.build());
        }
    }

    private void buildSetNull(TypeSpec.Builder builder) {
        if (optional.setterNullable()) {
            MethodSpec.Builder setNull = MethodSpec.methodBuilder("set" + name + "Null")
                    .addModifiers(Modifier.PUBLIC);
            if (mutable) {
                implementSetNull(setNull);
            }
            else {
                setNull.addStatement("assert(false) : \"$L is immutable\"", name);
            }
            builder.addMethod(setNull.build());
        }
    }

    // Accessors

    protected void implementGetter(MethodSpec.Builder getter) {
    }

    protected void implementIsNull(MethodSpec.Builder isNull) {
    }

    // Init

    public void implementInit(MethodSpec.Builder method) {
    }

    public void implementClear(MethodSpec.Builder method) {
    }

    // Copy

    public void implementCopyInit(MethodSpec.Builder method, String rhs) {
    }

    public void implementAssign(MethodSpec.Builder assignFrom, String rhs) {
    }

    // Mutate

    protected void implementSetter(MethodSpec.Builder setter, String rhs) {
    }

    protected void implementSetNull(MethodSpec.Builder isNull) {
    }

    protected void ensureStructAllocated(MethodSpec.Builder builder) {
    }

    // toString

    public void implementToString(MethodSpec.Builder stringify, String builder) {
        String privateAccess = privateAccess();
        if (hasIsNullBackStore()) {
            stringify.addStatement("$L.assignment($S, $L, $L)", builder, name, privateAccess, privateIsNullBackstoreAccess());
        }
        else {
            stringify.addStatement("$L.assignment($S, $L)", builder, name, privateAccess);
        }
    }

    // equals

    public void implementEqualsFalse(MethodSpec.Builder equate, String that) {
        if (getter != null) {
            String privateAccess = privateAccess();
            String publicAccess = publicAccess();

            String thisGetter = privateAccess;
            String thatGetter = that + "." + publicAccess;

            if (optional().isNullable()) {
                equate.addStatement("$T this$L = $L", privateAccessType(), name, thisGetter);
                equate.addStatement("$T that$L = $L", getter, name, thatGetter);
                thatGetter = "that" + name;
                thisGetter = "this" + name;
            }

            switch (storage) {
                case Primitive:
                    if (optional().isNullable()) {
                        equate.addStatement("boolean temp$LNull = $L", name, nullCondition());
                        equate.addStatement("if ((temp$LNull && !$L.$L) || (!temp$LNull && (" + conditionEqualsFalsePrimative(impl.getKind()) + "))) return false", name, that, publicIsNull(), name, thisGetter, thatGetter);
                    } else {
                        equate.addStatement("if (" + conditionEqualsFalsePrimative(impl.getKind()) + ") return false", thisGetter, thatGetter);
                    }
                    break;
                case Boxed:
                    if (optional().isNullable()) {
                        equate.addStatement("if (($L == null && $L != null) || ($L != null && !$L.equals($L))) return false", thisGetter, thatGetter, thisGetter, thisGetter, thatGetter);
                    } else {
                        equate.addStatement("if (!$L.equals($L)) return false", privateAccess, publicAccess);
                    }
                    break;
                case String:
                    if (optional().isNullable()) {
                        equate.addStatement("if (($L == null && $L != null) || ($L != null && !$L.equals($L))) return false", thisGetter, thatGetter, thisGetter, thisGetter, thatGetter);
                    } else {
                        equate.addStatement("if (!$L.equals($L)) return false", privateAccess, publicAccess);
                    }
                    break;
                case Structure:
                    if (optional().isNullable()) {
                        equate.addStatement("if (((" + nullCondition() + ") && ($L != null)) || (!(" + nullCondition() + ") && !$L.equals($L))) return false", thatGetter, thisGetter, thatGetter);
                    } else {
                        equate.addStatement("if (!$L.equals($L)) return false", privateAccess, publicAccess);
                    }
                    break;
                case Array:
                    // TODO: Array
                    break;
            }
        }
    }

    private String conditionEqualsFalsePrimative(TypeKind kind) {
        switch (kind) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case CHAR:
            case LONG:
                break;
            case FLOAT:
                return "Float.compare($L, $L) != 0";
            case DOUBLE:
                return "Double.compare($L, $L) != 0";
        }
        return "$L != $L";
    }

    // HashCode

    public void implementHashCode(MethodSpec.Builder hashify, String result) {
        String privateAccess = privateAccess();
        switch (storage) {
            case Primitive:
                // nullable has same effect
                implementHashCodePrimative(impl.getKind(), hashify, result);
                break;
            case Boxed:
                if (optional().isNullable()) {
                    hashify.addStatement("$T temp$L = $L", privateAccessType(), name, privateAccess);
                    hashify.addStatement("$L = 31 * $L + (temp$L == null ? 0: temp$L.hashCode())", result, result, name, name);
                }
                else {
                    hashify.addStatement("$L = 31 * $L + $L.hashCode()", result, result, privateAccess);
                }
                break;
            case String:
                if (optional().isNullable()) {
                    hashify.addStatement("$L = 31 * $L + (" + nullCondition() + " ? 0: $L.hashCode())", result, result, privateAccess);
                }
                else {
                    hashify.addStatement("$L = 31 * $L + $L.hashCode()", result, result, privateAccess);
                }
                break;
            case Structure:
                if (optional().isNullable()) {
                    hashify.addStatement("$L = 31 * $L + (" + nullCondition() + " ? 0: $L.hashCode())", result, result, privateAccess);
                }
                else {
                    hashify.addStatement("$L = 31 * $L + $L.hashCode()", result, result, privateAccess);
                }
                break;
            case Array:
                // TODO: Array
                break;
        }
    }

    private void implementHashCodePrimative(TypeKind kind, MethodSpec.Builder hashify, String result) {
        String privateAccess = privateAccess();
        switch (kind) {
            case BOOLEAN:
                hashify.addStatement("$L = 31 * $L + ($L ? 1231 : 1237)", result, result, privateAccess);
                break;
            case INT:
                hashify.addStatement("$L = 31 * $L + $L", result, result, privateAccess);
                break;
            case BYTE:
            case SHORT:
            case CHAR:
                hashify.addStatement("$L = 31 * $L + (int)$L", result, result, privateAccess);
                break;
            case LONG:
                hashify.addStatement("long temp$L = $L", name, privateAccess);
                hashify.addStatement("$L = 31 * $L + (int)(temp$L ^ (temp$L >>> 32))", result, result, name, name);
                break;
            case FLOAT:
                hashify.addStatement("float temp$L = $L", name, privateAccess);
                hashify.addStatement("$L = 31 * $L + (temp$L != +0.0f ? Float.floatToIntBits(temp$L) : 0)", result, result, name, name);
                break;
            case DOUBLE:
                hashify.addStatement("long temp$L = Double.doubleToLongBits($L)", name, privateAccess);
                hashify.addStatement("$L = 31 * $L + (int)(temp$L ^ (temp$L >>> 32))", result, result, name, name);
                break;
        }
    }

    public void implementRead(String input, MethodSpec.Builder builder) {
        if (optional().isNullable()) {
            builder.beginControlFlow("if ($L.readBoolean())", input);
        }
        switch (storage()) {
            case Primitive:
            case Boxed:
                String rhs = readValuePrimative(storage().getComponentKind(impl)).replace("$L", input);
                implementSetter(builder, rhs);
                break;
            case String:
                builder.addStatement("$L = $L.readUTF()", privateAccess(), input);
                break;
            case Structure:
                ensureStructAllocated(builder);
                builder.addStatement("$L.readExternal($L)", privateAccess(), input);
                break;
            case Array:
                // TODO: Array
                break;
        }
        if (optional().isNullable()) {
            builder.nextControlFlow("else");
            implementSetNull(builder);
            builder.endControlFlow();
        }
    }

    private String readValuePrimative(TypeKind kind) {
        switch (kind) {
            case BOOLEAN:
                return "$L.readBoolean()";
            case BYTE:
                return "$L.readByte()";
            case SHORT:
                return "$L.readShort()";
            case INT:
                return "$L.readInt()";
            case LONG:
                return "$L.readLong()";
            case CHAR:
                return "$L.readChar()";
            case FLOAT:
                return "$L.readFloat()";
            case DOUBLE:
                return "$L.readDouble()";
        }
        return "$L";
    }

    public void implementWrite(String output, MethodSpec.Builder builder) {
        if (optional().isNullable()) {
            builder.beginControlFlow("if ($L)", nullCondition());
            builder.addStatement("$L.writeBoolean(false)", output);
            builder.nextControlFlow("else");
            builder.addStatement("$L.writeBoolean(true)", output);
        }
        switch (storage()) {
            case Primitive:
            case Boxed:
                builder.addStatement(writeValuePrimative(storage().getComponentKind(impl)), output, privateAccess());
                break;
            case String:
                builder.addStatement("$L.writeUTF($L)", output, privateAccess());
                break;
            case Structure:
                builder.addStatement("$L.writeExternal($L)", privateAccess(), output);
                break;
            case Array:
                // TODO: Array
                break;
        }
        if (optional().isNullable()) {
            builder.endControlFlow();
        }
    }

    private String writeValuePrimative(TypeKind kind) {
        switch (kind) {
            case BOOLEAN:
                return "$L.writeBoolean($L)";
            case BYTE:
                return "$L.writeByte($L)";
            case SHORT:
                return "$L.writeShort($L)";
            case INT:
                return "$L.writeInt($L)";
            case LONG:
                return "$L.writeLong($L)";
            case CHAR:
                return "$L.writeChar($L)";
            case FLOAT:
                return "$L.writeFloat($L)";
            case DOUBLE:
                return "$L.writeDouble($L)";
        }
        return "ACCESSOR -> $L";
    }
}
