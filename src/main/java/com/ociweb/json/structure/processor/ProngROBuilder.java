package com.ociweb.json.structure.processor;

import com.ociweb.json.structure.annotations.ProngStruct;
import com.ociweb.json.structure.processor.property.PropertyBuilderBase;
import com.ociweb.json.structure.processor.property.PropertyBuilderFactory;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;

import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.List;

public class ProngROBuilder {
    private final ProcessingEnvironment env;
    private final TypeElement element;
    private final String suffix;

    private final ClassName interfaceName;
    private final ClassName roName;
    private final TypeSpec.Builder builder;
    private final PropertyBuilderFactory factory;

    private List<PropertyBuilderBase> orderedProperties;

    ProngROBuilder(ProcessingEnvironment env, TypeElement element, ProngStruct annotation, PropertyBuilderFactory factory) {
        this.env = env;
        this.element = element;
        this.suffix = "RO";
        this.factory = factory;
        Element enclosingElement = element.getEnclosingElement();
        PackageElement packageElement = (PackageElement)enclosingElement;
        this.roName = ClassName.get(packageElement.getQualifiedName().toString(), element.getSimpleName().toString() + "RO");
        this.interfaceName = ClassName.get(packageElement.getQualifiedName().toString(), element.getSimpleName().toString());

        this.builder = TypeSpec.classBuilder(roName)
                .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT);

        if (element.getKind() == ElementKind.INTERFACE) {
            this.builder.addSuperinterface(interfaceName);
        }
        else {
            this.builder.superclass(interfaceName);
        }
    }

    public void write(String indent) {
        ProngPropertyCollector collector = new ProngPropertyCollector(element, env, factory);
        orderedProperties = collector.collectProperties(suffix);

        buildAccessors();

        writeSourceFile(indent);
    }

    private void buildAccessors() {
        for (PropertyBuilderBase property : orderedProperties) {
            property.buildAbstractAccessors(builder);
        }
    }

    private void writeSourceFile(String indent) {
        Filer filer = env.getFiler();
        Messager messenger = env.getMessager();
        try {
            JavaFile.builder(this.roName.packageName(), builder.build())
                    .skipJavaLangImports(true)
                    .indent(indent)
                    .build()
                    .writeTo(filer);
        } catch (IOException e) {
            messenger.printMessage(Diagnostic.Kind.ERROR, e.getLocalizedMessage(), element);
        }
    }
}
