package com.ociweb.jfast.loader;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import com.ociweb.jfast.primitive.adapter.FASTInputStream;

public class TemplateLoaderTest {

	@Test
	public void buildRawCatalog() {
		byte[] catalogByteArray = buildRawCatalogData();
		
        //reconstruct Catalog object from stream		
		FASTInput input = new FASTInputByteArray(catalogByteArray);
		TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(input));
		
		// /performance/example.xml contains 3 templates.
		assertEquals(3, catalog.templatesCount());
		assertEquals(352, catalogByteArray.length);			
	}
	
	@Test
	public void testTwo() {	
		
		FASTInput input = new FASTInputByteArray(buildRawCatalogData());
		TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(input));
		
		URL sourceData = getClass().getResource("/performance/complex30000.dat");
		File fileSource = new File(sourceData.getFile());
			
		try {
			FASTInputStream fist = new FASTInputStream(new FileInputStream(fileSource)); 
			
			byte[] targetBuffer = new byte[10];
			fist.init(targetBuffer);
			fist.fill(0, 10);
			
//			System.err.println("DATA:"+hexString(targetBuffer));
			
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		
//		ByteArrayInputStream bais = new ByteArrayInputStream()
//		
//		
//		ByteArrayOutputStream sourceBuffer = new ByteArrayOutputStream(1<<22);
		
		
		
	}

	
	private String hexString(byte[] targetBuffer) {
		StringBuilder builder = new StringBuilder();
		
		for(byte b:targetBuffer) {
			
			String tmp = Integer.toHexString(0xFF&b);
			builder.append(tmp.substring(Math.max(0, tmp.length()-2))).append(" ");
			
		}
		
		
		return builder.toString();
	}

	private byte[] buildRawCatalogData() {
		URL source = getClass().getResource("/performance/example.xml");
			
		
		ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
		File fileSource = new File(source.getFile());
		try {			
			TemplateLoader.buildCatalog(catalogBuffer, fileSource);
		} catch (ParserConfigurationException | SAXException | IOException e) {
			e.printStackTrace();
		}
		
		assertTrue("Catalog must be built.",catalogBuffer.size()>0);
		
		byte[] catalogByteArray = catalogBuffer.toByteArray();
		return catalogByteArray;
	}
	
}
