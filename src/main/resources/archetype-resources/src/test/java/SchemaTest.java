package ${package};

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import $(package}.SchemaOneSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaTest {

	@Test
    public void messageClientNetResponseSchemaFROMTest() {
    	
        assertTrue(FROMValidation.checkSchema("/SchemaOne.xml", SchemaOneSchema.class));
    }
	
}
