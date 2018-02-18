package com.ociweb.json.structure.processor;

import com.ociweb.json.structure.annotations.ProngProperty;
import com.ociweb.json.structure.processor.property.PropertyBuilderBase;
import com.ociweb.json.structure.processor.property.PropertyBuilderFactory;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import java.util.*;

public class ProngPropertyCollector {
    private final TypeElement element;
    private ProcessingEnvironment env;
    private final List<PropertyBuilderBase> orderedProperties = new ArrayList<>();
    private final Map<String, PropertyBuilderBase> lookupProperties = new HashMap<>();
    private final Set<String> badPropeerties = new HashSet<>();
    private PropertyBuilderFactory factory;

    public ProngPropertyCollector(TypeElement element, ProcessingEnvironment env, PropertyBuilderFactory factory) {
        this.element = element;
        this.env = env;
        this.factory = factory;
    }

    public List<PropertyBuilderBase> collectProperties(String suffix) {
        collectProperties(element, suffix);
        return orderedProperties;
    }

    private void collectProperties(TypeElement element, String suffix) {
        TypeMirror superclass = element.getSuperclass();
        if (superclass  != null) {
            TypeElement superElement = (TypeElement) env.getTypeUtils().asElement(superclass);
            if (superElement != null) {
                collectProperties(superElement, suffix);
            }
        }
        for (TypeMirror adheresTo : element.getInterfaces()) {
            TypeElement adheresToElement = (TypeElement) env.getTypeUtils().asElement(adheresTo);
            if (adheresToElement != null) {
                collectProperties(adheresToElement, suffix);
            }
        }
        collectMethodProperties(element, suffix);
    }

    private void collectMethodProperties(TypeElement element, String suffix) {
        for (Element enclosed : element.getEnclosedElements()) {
            if (enclosed.getKind() == ElementKind.METHOD) {
                // if abstract getX, setX, or isXNull
                ExecutableElement methodElement = (ExecutableElement) enclosed;
                MethodType methodType = ExecutiveElementParser.fetchAccessorStyle(methodElement);
                if (methodType == null) {
                    continue; // drop
                }

                // extract property name
                String name = null;
                switch (methodType) {
                    case Getter:
                    case Setter:
                        name = methodElement.getSimpleName().toString().substring(3);
                        break;
                    case IsNull:
                        name = methodElement.getSimpleName().toString();
                        name = name.substring(2, name.length()-4);
                        break;
                }

                if (badPropeerties.contains(name)) {
                    continue; // drop
                }

                // Find or create property with name
                PropertyBuilderBase property = lookupProperties.get(name);
                if (property == null) {
                    property = factory.createPropertyBuilder(env, name, suffix);
                    lookupProperties.put(name, property);
                    orderedProperties.add(property);
                }

                // Imbue method info into property
                TypeMirror mirror = ExecutiveElementParser.fetchTypeMirror(methodElement, methodType);
                ProngProperty propAnnotation = methodElement.getAnnotation(ProngProperty.class);
                String error = property.discovered(methodType, mirror, propAnnotation);
                if (error != null) {
                    env.getMessager().printMessage(Diagnostic.Kind.ERROR,
                            "Invalid Property: " + error,
                            methodElement);
                    orderedProperties.remove(property);
                    lookupProperties.remove(name);
                    badPropeerties.add(name);
                }
            }
        }
    }
}
