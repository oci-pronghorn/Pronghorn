package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SequentialFileIOSSchemaTest {

    @Test
    public void testRequestSchemaFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/fileIORequest.xml", SequentialFileIORequestSchema.class));
    }
 
    
    @Test
    public void testResponseSchemaFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/fileIOResponse.xml", SequentialFileIOResponseSchema.class));
    }

    
}
