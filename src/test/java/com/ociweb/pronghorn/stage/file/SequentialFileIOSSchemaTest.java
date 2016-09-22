package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;
import com.ociweb.pronghorn.stage.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.stage.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.stage.network.schema.ServerRequestSchema;
import com.ociweb.pronghorn.stage.network.schema.ServerResponseSchema;

public class SequentialFileIOSSchemaTest {

    @Test
    public void testRequestSchemaFROMMatchesXML() {
        assertTrue(FROMValidation.testForMatchingFROMs("/fileIORequest.xml", SequentialFileIORequestSchema.instance));
    };
    
    @Test
    public void testRequestSchemaConstantFields() {
        assertTrue(FROMValidation.testForMatchingLocators(SequentialFileIORequestSchema.instance));
    }    
    
    @Test
    public void testResponseSchemaFROMMatchesXML() {
        assertTrue(FROMValidation.testForMatchingFROMs("/fileIOResponse.xml", SequentialFileIOResponseSchema.instance));
    };
    
    @Test
    public void testResponseSchemaConstantFields() {
        assertTrue(FROMValidation.testForMatchingLocators(SequentialFileIOResponseSchema.instance));
    }  
    
}
