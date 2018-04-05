package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;
import com.ociweb.pronghorn.stage.file.schema.BlockManagerRequestSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockManagerResponseSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageReceiveSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageXmitSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadConsumerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadProducerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreConsumerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreProducerSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialCtlSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialRespSchema;

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
	public void testPersistedBlobLoadConsumerSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "PersistedBlobLoadConsumer.xml", PersistedBlobLoadConsumerSchema.class));
	}
	
	@Test
	public void testPersistedBlobLoadProducerSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "PersistedBlobLoadProducer.xml", PersistedBlobLoadProducerSchema.class));
	}

	@Test
	public void testPersistedBlobSaveConsumerSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "PersistedBlobStoreConsumer.xml", PersistedBlobStoreConsumerSchema.class));

	}
	
	@Test
	public void testPersistedBlobSaveProducerSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "PersistedBlobStoreProducer.xml", PersistedBlobStoreProducerSchema.class));

	}
	
	@Test
	public void testSequentialFileControlSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "SequentialCtl.xml", SequentialCtlSchema.class));
	}
	
	@Test
	public void testSequentialFileResponseSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "SequentialResp.xml", SequentialRespSchema.class));
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
