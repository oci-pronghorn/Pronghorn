package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaValidation {
    
    @Test
    public void groveResponseFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/rawDataSchema.xml", RawDataSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(RawDataSchema.instance));
    }
    
}
