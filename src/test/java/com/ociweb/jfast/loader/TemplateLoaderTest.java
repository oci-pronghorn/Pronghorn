package com.ociweb.jfast.loader;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.Arrays;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;

public class TemplateLoaderTest {

	@Test
	public void loadTest() {
		URL source = getClass().getResource("/performance/example.xml");
		
		
		
		ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
		File fileSource = new File(source.getFile());
		try {			
			TemplateLoader.buildCatalog(catalogBuffer, fileSource);
		} catch (ParserConfigurationException | SAXException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertTrue("Catalog must be built.",catalogBuffer.size()>0);
//		System.err.println("cat bytes length "+catalogBuffer.size());
		
        //reconstruct Catalog object from stream		
		FASTInput input = new FASTInputByteArray(catalogBuffer.toByteArray());
//		Catalog catalog = new Catalog(new PrimitiveReader(input));
//		
//		//TODO: confirm that the catalog contains the right data.
//		int[] tokens = catalog.tokens;
//		System.err.println(Arrays.toString(tokens));
		
	}
	
}
