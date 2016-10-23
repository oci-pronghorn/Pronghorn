package com.ociweb.pronghorn.stage.network;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.network.schema.ServerRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class ServerSchemaTest {

    @Test
    public void testServerRequestSchemaFROMMatchesXML() {
        assertTrue(FROMValidation.testForMatchingFROMs("/serverRequest.xml", ServerRequestSchema.instance));
    };
    
    @Test
    public void testServerRequestSchemaConstantFields() {
        assertTrue(FROMValidation.testForMatchingLocators(ServerRequestSchema.instance));
    }
    
    
    @Test
    public void testServerResponseSchemaFROMMatchesXML() {
        assertTrue(FROMValidation.testForMatchingFROMs("/serverResponse.xml", ServerResponseSchema.instance));
    };
    
    @Test
    public void testServerResponseSchemaConstantFields() {
        assertTrue(FROMValidation.testForMatchingLocators(ServerResponseSchema.instance));
    }  
    
    @Test
    public void testServerConnectFROMMatchesXML() {
        assertTrue(FROMValidation.testForMatchingFROMs("/serverConnect.xml", ServerConnectionSchema.instance));
    };
    
    @Test
    public void testServerConnectConstantFields() {
        assertTrue(FROMValidation.testForMatchingLocators(ServerConnectionSchema.instance));
    }

    @Test
    public void testHTTPRequestFROMMatchesXML() {
        assertTrue(FROMValidation.testForMatchingFROMs("/httpRequest.xml", HTTPRequestSchema.instance));
    };
    
    @Test
    public void testHTTPRequestConstantFields() {
        assertTrue(FROMValidation.testForMatchingLocators(HTTPRequestSchema.instance));
    }
    
    
    
}
