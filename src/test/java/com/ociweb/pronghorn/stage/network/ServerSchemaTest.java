package com.ociweb.pronghorn.stage.network;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.network.schema.HTTPLogRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPLogResponseSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class ServerSchemaTest {


    @Test
    public void testServerResponseSchemaFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/serverResponse.xml", ServerResponseSchema.class));
    }

    @Test
    public void testServerConnectFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/serverConnect.xml", ServerConnectionSchema.class));
    }

    @Test
    public void testHTTPRequestFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/HTTPRequest.xml", HTTPRequestSchema.class));
    }

    @Test
    public void testHTTPLogRequestFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/HTTPLogRequest.xml", HTTPLogRequestSchema.class));
    }   
    
    @Test
    public void testHTTPLogResponseFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/HTTPLogResponse.xml", HTTPLogResponseSchema.class));
    }
    
    
}
