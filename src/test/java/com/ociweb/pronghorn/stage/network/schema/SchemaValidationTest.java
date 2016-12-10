package com.ociweb.pronghorn.stage.network.schema;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetRequestSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaValidationTest {

	
    @Test
    public void messageClientNetResponseSchemaFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/NetPayload.xml", NetPayloadSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(NetPayloadSchema.instance));
    }
    
    @Test
    public void messageNetParseAckSchemaFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/Release.xml", ReleaseSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(ReleaseSchema.instance));
    }
	
    @Test
    public void messageNetResponseSchemaFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/NetResponse.xml", NetResponseSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(NetResponseSchema.instance));
    }
	    
    @Test
    public void messageNetRequestSchemaFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/NetRequest.xml", NetRequestSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(NetRequestSchema.instance));
    }
    
}
