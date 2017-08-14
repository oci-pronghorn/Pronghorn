package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;
import com.ociweb.pronghorn.stage.file.schema.BlockManagerRequestSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockManagerResponseSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileIORequestSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileIOResponseSchema;

import java.io.File;

public class SchemaTest {

	@Test
	public void testBlockManagerRequestSchema() {
		assertTrue(FROMValidation.checkSchema("src" + File.separator + "test" + File.separator + "resources" + File.separator + "BlockManagerRequest.xml", BlockManagerRequestSchema.class));
	}

	@Test
	public void testBlockManagerResponseSchema() {
		assertTrue(FROMValidation.checkSchema("src" + File.separator + "test" + File.separator + "resources" + File.separator + "BlockManagerResponse.xml", BlockManagerResponseSchema.class));
	}

	@Test
	public void testPersistedBlobLoadSchema() {
		assertTrue(FROMValidation.checkSchema("src" + File.separator + "test" + File.separator + "resources" + File.separator + "PersistedBlobLoad.xml", PersistedBlobLoadSchema.class));
	}

	@Test
	public void testPersistedBlobSaveSchema() {
		assertTrue(FROMValidation.checkSchema("src" + File.separator + "test" + File.separator + "resources" + File.separator + "PersistedBlobStore.xml", PersistedBlobStoreSchema.class));

	}


	@Test
	public void testSequentialFileIORequestSchema() {//under development
		assertTrue(FROMValidation.checkSchema("src" + File.separator + "test" + File.separator + "resources" + File.separator + "fileIORequest.xml", SequentialFileIORequestSchema.class));
	}

	@Test
	public void testSequentialFileIOResponseSchema() {//under development
		assertTrue(FROMValidation.checkSchema( "src" + File.separator + "test" + File.separator + "resources" + File.separator + "fileIOResponse.xml", SequentialFileIOResponseSchema.class));
	}


}
