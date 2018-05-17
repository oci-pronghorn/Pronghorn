package ${package};

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import ${package}.SchemaOneSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaTest {

    /**
     * Verify that the schema XML matches with the generated class.
     * If not, the correct output will be displayed in the console for copying.
     */
	@Test
    public void messageClientNetResponseSchemaFROMTest() {
    	
        assertTrue(FROMValidation.checkSchema("/SchemaOne.xml", SchemaOneSchema.class));

    }
	
}
