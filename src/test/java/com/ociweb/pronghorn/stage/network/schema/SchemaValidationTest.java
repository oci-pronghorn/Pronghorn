package com.ociweb.pronghorn.stage.network.schema;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientRequestSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientResponseSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchema;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.network.schema.MQTTServerToClientSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaValidationTest {

	
    @Test
    public void messageClientNetResponseSchemaFROMTest() {
        assertTrue(FROMValidation.checkSchema("/NetPayload.xml", NetPayloadSchema.class));
    }
    
    @Test
    public void messageNetParseAckSchemaFROMTest() {
        assertTrue(FROMValidation.checkSchema("/Release.xml", ReleaseSchema.class));
    }
	
    @Test
    public void messageNetResponseSchemaFROMTest() {
        assertTrue(FROMValidation.checkSchema("/NetResponse.xml", NetResponseSchema.class));
    }
	    
    @Test
    public void messageNetRequestSchemaFROMTest() {
        assertTrue(FROMValidation.checkSchema("/ClientHTTPRequest.xml", ClientHTTPRequestSchema.class));
    }
    
    @Test
    public void messageMQTTIdRangeSchemaFROMTest() {
        assertTrue(FROMValidation.checkSchema("/MQTTIdRanges.xml", MQTTIdRangeSchema.class));
    }
    
    @Test
    public void messageMQTTClientToServerTest() {
        assertTrue(FROMValidation.checkSchema("/MQTTClientToServer.xml", MQTTClientToServerSchema.class));
    }
    
    @Test
    public void messageMQTTServerToClientTest() {
        assertTrue(FROMValidation.checkSchema("/MQTTServerToClient.xml", MQTTServerToClientSchema.class));
    }
    
    @Test
    public void messageMQTTClientRequestTest() {
        assertTrue(FROMValidation.checkSchema("/MQTTClientRequest.xml", MQTTClientRequestSchema.class));
    }
    
    @Test
    public void messageMQTTClientResponseTest() {
        assertTrue(FROMValidation.checkSchema("/MQTTClientResponse.xml", MQTTClientResponseSchema.class));
    }
    
}
