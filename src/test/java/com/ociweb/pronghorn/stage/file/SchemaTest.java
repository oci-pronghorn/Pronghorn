package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;
import com.ociweb.pronghorn.stage.file.schema.BlockManagerRequestSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockManagerResponseSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageReceiveSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageXmitSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileControlSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileResponseSchema;

public class SchemaTest {

	private static final String ROOT = "src" + File.separator + "test" + File.separator + "resources" + File.separator;

	@Test
	public void testBlockManagerRequestSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "BlockManagerRequest.xml", BlockManagerRequestSchema.class));
	}

	@Test
	public void testBlockManagerResponseSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "BlockManagerResponse.xml", BlockManagerResponseSchema.class));
	}

	@Test
	public void testPersistedBlobLoadSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "PersistedBlobLoad.xml", PersistedBlobLoadSchema.class));
	}

	@Test
	public void testPersistedBlobSaveSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "PersistedBlobStore.xml", PersistedBlobStoreSchema.class));

	}
	
	@Test
	public void testSequentialFileControlSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "SequentialFileControl.xml", SequentialFileControlSchema.class));
	}
	
	@Test
	public void testSequentialFileResponseSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "SequentialFileResponse.xml", SequentialFileResponseSchema.class));
	}
	
	@Test
	public void testBlockStorageXmitSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "BlockStorageXmit.xml", BlockStorageXmitSchema.class));
	}
	
	@Test
	public void testBlockStorageReceiveSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "BlockStorageReceive.xml", BlockStorageReceiveSchema.class));
	}
	
	
	
//
//	

}
