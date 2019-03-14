package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaValidation {
    
    @Test
    public void rawDataFROMTest() {
        assertTrue(FROMValidation.checkSchema("/rawDataSchema.xml", RawDataSchema.class));
        
        int testSlabSize = 128;
        int computedCountOfChunks = FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(RawDataSchema.FROM, testSlabSize);
        int expected = testSlabSize/4; //4 ints for 1 chunk
        assertEquals(expected, computedCountOfChunks);

    }
        
    @Test
    public void rawDataTest() {
        assertTrue(FROMValidation.checkSchema("/rawDataSchema.xml", RawDataSchema.class));
    }
    
    
    @Test
    public void testDataTest() {
        assertTrue(FROMValidation.checkSchema("/testDataSchema.xml", TestDataSchema.class));
    }   
    
}
