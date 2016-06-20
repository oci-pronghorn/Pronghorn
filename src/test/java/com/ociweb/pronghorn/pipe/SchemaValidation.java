package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaValidation {
    
    @Test
    public void rawDataFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/rawDataSchema.xml", RawDataSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(RawDataSchema.instance));
        
        //TODO: new development for building interfaces.
        try {
            FieldReferenceOffsetManager.buildFROMInterfaces(System.out, RawDataSchema.class.getSimpleName(), RawDataSchema.FROM);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
}
