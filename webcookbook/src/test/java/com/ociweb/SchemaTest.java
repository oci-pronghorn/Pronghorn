package com.ociweb;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import com.ociweb.ConnectionData;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaTest {

	@Test
    public void messageClientNetResponseSchemaFROMTest() {
    	
        assertTrue(FROMValidation.checkSchema("/ConnectionDataSchema.xml", ConnectionData.class));
    }
	
}
