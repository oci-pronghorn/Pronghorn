package com.ociweb.pronghorn.structure.processor.property;

import javax.annotation.processing.ProcessingEnvironment;

public interface PropertyBuilderFactory {
    boolean isMutable();
    boolean implementsEqualHash();
    boolean implementsToString();
    boolean isSerializable();
    boolean isExternalizable();

    PropertyBuilderBase createPropertyBuilder(ProcessingEnvironment env, String name, String suffix);
}

