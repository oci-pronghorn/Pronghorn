package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileIORequestSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileIOResponseSchema;

public class SchemaTest {

	@Test
	public void testPersistedBlobLoadSchema() {
		  assertTrue(FROMValidation.checkSchema("/PersistedBlobLoad.xml", PersistedBlobLoadSchema.class));
	}
	
	@Test
	public void testPersistedBlobSaveSchema() {
		  assertTrue(FROMValidation.checkSchema("/PersistedBlobStore.xml", PersistedBlobStoreSchema.class));

	}
	
	
    @Test
    public void testSequentialFileIORequestSchema() {//under development
        assertTrue(FROMValidation.checkSchema("/fileIORequest.xml", SequentialFileIORequestSchema.class));
    }
    
    @Test
    public void testSequentialFileIOResponseSchema() {//under development
        assertTrue(FROMValidation.checkSchema("/fileIOResponse.xml", SequentialFileIOResponseSchema.class));
    }

    
}
