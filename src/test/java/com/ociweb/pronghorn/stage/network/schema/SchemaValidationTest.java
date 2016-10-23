package com.ociweb.pronghorn.stage.network.schema;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.network.schema.ClientNetRequestSchema;
import com.ociweb.pronghorn.network.schema.ClientNetResponseSchema;
import com.ociweb.pronghorn.network.schema.NetParseAckSchema;
import com.ociweb.pronghorn.network.schema.NetRequestSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaValidationTest {

    @Test
    public void messageClientNetRequestSchemaFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/ClientNetRequest.xml", ClientNetRequestSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(ClientNetRequestSchema.instance));
    }
	
    @Test
    public void messageClientNetResponseSchemaFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/ClientNetResponse.xml", ClientNetResponseSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(ClientNetResponseSchema.instance));
    }
    
    @Test
    public void messageNetParseAckSchemaFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/NetParseAck.xml", NetParseAckSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(NetParseAckSchema.instance));
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
