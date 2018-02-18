package com.ociweb.json.structure.processor.property.readonly;

import com.ociweb.json.structure.processor.property.PropertyBuilderBase;
import com.ociweb.json.structure.processor.property.PropertyBuilderFactory;

import javax.annotation.processing.ProcessingEnvironment;

public class ReadonlyPropertyBuilderFactory implements PropertyBuilderFactory {

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public boolean implementsEqualHash() {
        return false;
    }

    @Override
    public boolean implementsToString() {
        return false;
    }

    @Override
    public boolean isSerializable() {
        return false;
    }

    @Override
    public boolean isExternalizable() {
        return false;
    }

    @Override
    public PropertyBuilderBase createPropertyBuilder(ProcessingEnvironment env, String name, String suffix) {
        return new PropertyBuilderBase(env, name, isMutable());
    }
}
