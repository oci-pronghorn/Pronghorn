package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaValidation {
    
    @Test
    public void rawDataFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/rawDataSchema.xml", RawDataSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(RawDataSchema.instance));
        
        int testSlabSize = 128;
        int computedCountOfChunks = FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(RawDataSchema.FROM, testSlabSize);
        int expected = testSlabSize/4; //4 ints for 1 chunk
        assertEquals(expected, computedCountOfChunks);

    }
    
}
