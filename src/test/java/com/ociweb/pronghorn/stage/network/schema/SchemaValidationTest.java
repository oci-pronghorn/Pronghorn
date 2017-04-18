package com.ociweb.pronghorn.stage.network.schema;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.MQTTConnectionInSchema;
import com.ociweb.pronghorn.network.schema.MQTTConnectionOutSchema;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
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
        assertTrue(FROMValidation.testForMatchingFROMs("/ClientHTTPRequest.xml", ClientHTTPRequestSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(ClientHTTPRequestSchema.instance));
    }
    
    @Test
    public void messageMQTTIdRangeSchemaFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/MQTTIdRanges.xml", MQTTIdRangeSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(MQTTIdRangeSchema.instance));
    }
    
    @Test
    public void messageMQTTConnectionInSchemaFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/MQTTConnectionIn.xml", MQTTConnectionInSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(MQTTConnectionInSchema.instance));
    }
    
    @Test
    public void messageMQTTConnectionOutSchemaFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/MQTTConnectionOut.xml", MQTTConnectionOutSchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(MQTTConnectionOutSchema.instance));
    }
    
}
