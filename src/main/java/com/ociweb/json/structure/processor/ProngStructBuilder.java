package com.ociweb.json.structure.processor;

import com.ociweb.json.structure.annotations.ProngStruct;
import com.ociweb.json.structure.annotations.ProngStructFormatting;
import com.ociweb.json.structure.annotations.ProngStructFormatter;
import com.ociweb.json.structure.processor.property.*;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.tools.Diagnostic;
import java.io.*;
import java.util.List;

public class ProngStructBuilder {
    private final ProcessingEnvironment env;
    private final TypeElement element;
    private final String suffix;

    private final ClassName interfaceName;
    private final ClassName implName;
    private final ClassName roName;
    private final TypeSpec.Builder builder;
    private final PropertyBuilderFactory factory;

    private List<PropertyBuilderBase> orderedProperties;

    ProngStructBuilder(ProcessingEnvironment env, TypeElement element, ProngStruct annotation, PropertyBuilderFactory factory) {
        this.env = env;
        this.element = element;
        this.suffix = annotation.suffix();
        this.factory = factory;
        Element enclosingElement = element.getEnclosingElement();
        PackageElement packageElement = (PackageElement)enclosingElement;
        this.interfaceName = ClassName.get(packageElement.getQualifiedName().toString(), element.getSimpleName().toString());
        this.implName = ClassName.get(packageElement.getQualifiedName().toString(), element.getSimpleName().toString() + this.suffix);
        this.roName = ClassName.get(packageElement.getQualifiedName().toString(), element.getSimpleName().toString() + "RO");

        this.builder = TypeSpec.classBuilder(implName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        this.builder.superclass(roName);

        if (factory.implementsToString()) {
            builder.addSuperinterface(ProngStructFormatting.class);
        }

        if (factory.isExternalizable()) {
            builder.addSuperinterface(Externalizable.class);
        }

        if (factory.isSerializable()) {
            builder.addSuperinterface(Serializable.class);
        }
    }

    public void write(String indent) {
        ProngPropertyCollector collector = new ProngPropertyCollector(element, env, factory);
        orderedProperties = collector.collectProperties(suffix);

        postAnalysis();

        defaultConstructor();
        if (factory.isMutable()) {
            clearOut();
        }
        copyConstructor();
        if (factory.isMutable()) {
            assignFrom();
        }

        buildAccessors();

        if (factory.implementsToString()) {
            stringify();
            stringifyScoped();
            stringifyProperties();
        }
        if (factory.implementsEqualHash()) {
            equalizer();
            hashCoder();
        }
        if (factory.isSerializable()) {
            externalizableRead();
            externalizableWrite();
        }

        writeSourceFile(indent);
    }

    public ClassName interfaceName() {
        return interfaceName;
    }

    public ClassName implName() {
        return implName;
    }

    public Element element() {
        return element;
    }

    // Completion

    private void postAnalysis() {
        for (PropertyBuilderBase property : orderedProperties) {
            if (property.storage() == null) {
                env.getMessager().printMessage(Diagnostic.Kind.ERROR,
                        "Invalid Property Type: " + property);
            }
        }

        RecursionCheck check = new RecursionCheck(env.getTypeUtils(), env.getMessager());
        check.recursionCheck((TypeElement)element);
    }

    private void writeSourceFile(String indent) {
        Filer filer = env.getFiler();
        Messager messenger = env.getMessager();
        try {
            JavaFile.builder(this.implName.packageName(), builder.build())
                    .skipJavaLangImports(true)
                    .indent(indent)
                    .build()
                    .writeTo(filer);
        } catch (IOException e) {
            messenger.printMessage(Diagnostic.Kind.ERROR, e.getLocalizedMessage(), element);
        }
    }

// Construction Impls

    private void defaultConstructor() {
        MethodSpec.Builder method = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC);
        for (PropertyBuilderBase property : orderedProperties) {
            property.implementInit(method);
        }
        this.builder.addMethod(method.build());
    }

    private void clearOut() {
        MethodSpec.Builder method = MethodSpec.methodBuilder("clear")
                .addModifiers(Modifier.PUBLIC);
        MethodMatch match = ExecutiveElementParser.matchMethod(env.getTypeUtils(), element, method);
        if (match.isOverride) {
            method.addAnnotation(Override.class);
        }
        if (match.canInvokeSuper) {
            method.addStatement("super.clear()");
        }
        for (PropertyBuilderBase property : orderedProperties) {
            property.implementClear(method);
        }
        this.builder.addMethod(method.build());
    }

    private void copyConstructor() {
        MethodSpec.Builder method = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(interfaceName, "rhs");
        MethodMatch match = ExecutiveElementParser.matchMethod(env.getTypeUtils(), element, method);
        if (match.canInvokeSuper) {
            method.addStatement("super(rhs)");
        }
        for (PropertyBuilderBase property : orderedProperties) {
            property.implementCopyInit(method, "rhs");
        }
        this.builder.addMethod(method.build());
    }

    private void assignFrom() {
        MethodSpec.Builder method = MethodSpec.methodBuilder("assignFrom")
                .addModifiers(Modifier.PUBLIC)
                .addParameter(interfaceName, "rhs");
        MethodMatch match = ExecutiveElementParser.matchMethod(env.getTypeUtils(), element, method);
        match.isOverride = false; // TODO: remove this when matchMethod works
        if (match.isOverride) {
            method.addAnnotation(Override.class);
        }
        if (match.canInvokeSuper) {
            method.addStatement("super.assignFrom(rhs)");
        }
        for (PropertyBuilderBase property : orderedProperties) {
            property.implementAssign(method, "rhs");
        }
        this.builder.addMethod(method.build());
    }

    private void buildAccessors() {
        for (PropertyBuilderBase property : orderedProperties) {
            property.buildAccessors(builder);
        }
    }

// Struct Methods

    private void stringify() {
        MethodSpec.Builder method = MethodSpec.methodBuilder("toString")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .addStatement("final $T sb = new $T()", ProngStructFormatter.class, ProngStructFormatter.class)
                .addStatement("this.toString(sb)")
                .addStatement("return sb.toString()")
                .returns(String.class);
        MethodMatch match = ExecutiveElementParser.matchMethod(env.getTypeUtils(), element, method);
        if (!match.isOverride) {
            // only implement if base class does not have one
            this.builder.addMethod(method.build());
        }
    }

    private void stringifyScoped() {
        MethodSpec.Builder method = MethodSpec.methodBuilder("toString")
                .addModifiers(Modifier.PUBLIC)
                .addParameter(ProngStructFormatter.class, "sb");
        MethodMatch match = ExecutiveElementParser.matchMethod(env.getTypeUtils(), element, method);
        if (match.isOverride) {
            method.addAnnotation(Override.class);
        }
        // toStringProperties calls super
        method.addStatement("sb.beginScope($S)", element.getSimpleName().toString());
        method.addStatement("this.toStringProperties(sb)");
        method.addStatement("sb.endScope()");
        this.builder.addMethod(method.build());
    }

    private void stringifyProperties() {
        MethodSpec.Builder method = MethodSpec.methodBuilder("toStringProperties")
                .addModifiers(Modifier.PUBLIC)
                .addParameter(ProngStructFormatter.class, "sb");
        MethodMatch match = ExecutiveElementParser.matchMethod(env.getTypeUtils(), element, method);
        if (match.isOverride) {
            method.addAnnotation(Override.class);
        }
        if (match.canInvokeSuper) {
            method.addStatement("super.toStringProperties(sb)");
        }
        for (PropertyBuilderBase property : orderedProperties) {
            property.implementToString(method, "sb");
        }
        this.builder.addMethod(method.build());
    }

    private void equalizer() {
        MethodSpec.Builder method = MethodSpec.methodBuilder("equals")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .addParameter(Object.class, "o")
                .returns(boolean.class);
        MethodMatch match = ExecutiveElementParser.matchMethod(env.getTypeUtils(), element, method);
        if (match.canInvokeSuper && match.canReturnSuper) {
            method
                    .addStatement("if (!super.equals(o)) return false")
                    .addStatement("if (!(o instanceof $T)) return false", interfaceName)
                    .addStatement("$T that = ($T)o", interfaceName, interfaceName);
        } else {
            method
                    .addStatement("if (this == o) return true")
                    .addStatement("if (o == null || !(o instanceof $T)) return false", interfaceName)
                    .addStatement("$T that = ($T)o", interfaceName, interfaceName);
        }
        for (PropertyBuilderBase property : orderedProperties) {
            property.implementEqualsFalse(method, "that");
        }
        method.addStatement("return true");
        this.builder.addMethod(method.build());
    }

    private void hashCoder() {
        MethodSpec.Builder method = MethodSpec.methodBuilder("hashCode")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(int.class);
        MethodMatch match = ExecutiveElementParser.matchMethod(env.getTypeUtils(), element, method);
        if (match.canInvokeSuper && match.canReturnSuper) {
            method.addStatement("int result = super.hashCode()");
        } else {
            method.addStatement("int result = 0");
        }
        for (PropertyBuilderBase property : orderedProperties) {
            property.implementHashCode(method, "result");
        }
        method.addStatement("return result");
        this.builder.addMethod(method.build());
    }

// Serialize

    private void externalizableRead() {
        MethodSpec.Builder method = MethodSpec.methodBuilder("readExternal")
                .addModifiers(Modifier.PUBLIC)
                .addException(IOException.class)
                .addException(ClassNotFoundException.class)
                .addParameter(ObjectInput.class, "input");

        MethodMatch match = ExecutiveElementParser.matchMethod(env.getTypeUtils(), element, method);
        if (match.isOverride) {
            method.addAnnotation(Override.class);
        }
        if (match.canInvokeSuper) {
            method.addStatement("super.readExternal(input)");
        }
        for (PropertyBuilderBase property : orderedProperties) {
            property.implementRead( "input", method);
        }
        this.builder.addMethod(method.build());
    }

    private void externalizableWrite() {
        MethodSpec.Builder method = MethodSpec.methodBuilder("writeExternal")
                .addModifiers(Modifier.PUBLIC)
                .addException(IOException.class)
                .addParameter(ObjectOutput.class, "output");
        MethodMatch match = ExecutiveElementParser.matchMethod(env.getTypeUtils(), element, method);
        if (match.isOverride) {
            method.addAnnotation(Override.class);
        }
        if (match.canInvokeSuper) {
            method.addStatement("super.writeExternal(output)");
        }
        for (PropertyBuilderBase property : orderedProperties) {
            property.implementWrite("output", method);
        }
        this.builder.addMethod(method.build());
    }
}
