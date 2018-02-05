package com.ociweb.pronghorn.structure.processor.property.pojo;

import com.ociweb.pronghorn.structure.processor.property.PropertyBuilderBase;
import com.ociweb.pronghorn.structure.processor.property.PropertyBuilderFactory;

import javax.annotation.processing.ProcessingEnvironment;

public class PojoPropertyBuilderFactory implements PropertyBuilderFactory {

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public boolean implementsEqualHash() {
        return true;
    }

    @Override
    public boolean implementsToString() {
        return true;
    }

    @Override
    public boolean isSerializable() {
        return true;
    }

    @Override
    public boolean isExternalizable() {
        return true;
    }

    @Override
    public PropertyBuilderBase createPropertyBuilder(ProcessingEnvironment env, String name, String suffix) {
        return new PojoPropertyBuilder(env, name, suffix, isMutable());
    }
}

