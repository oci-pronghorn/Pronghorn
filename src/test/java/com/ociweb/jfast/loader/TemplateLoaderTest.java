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

import com.ociweb.jfast.field.TokenBuilder;
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
		
		long[] script = catalog.templateScript(2);
		
		System.err.println(convertScriptToString(script));
		
		
	}

	private String convertScriptToString(long[] script) {
		StringBuilder builder = new StringBuilder();
		for(long val:script) {
			int id = (int)(val>>>32);
			int token = (int)(val&0xFFFFFFFF);
			
			if (id>=0) {
				builder.append('[').append(id).append(']');
			} else {
				builder.append("CMD:");
			}
			builder.append(TokenBuilder.tokenToString(token));
			
			builder.append("\n");
		}
		return builder.toString();
	}
	
	@Test
	public void testTwo() {	
		
		FASTInput input = new FASTInputByteArray(buildRawCatalogData());
		TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(input));
		

		
		URL sourceData = getClass().getResource("/performance/complex30000.dat");
		File fileSource = new File(sourceData.getFile());
			
		try {
			FASTInputStream fist = new FASTInputStream(new FileInputStream(fileSource)); 
			
			//TODO: print expected template for 2
			
//			PrimitiveReader pr = new PrimitiveReader(fist);
//			byte[] targetBuffer = new byte[4];
//			pr.readByteData(targetBuffer, 0, 4);
//						
//			System.err.println("DATA:"+hexString(targetBuffer));
//			System.err.println("DATA:"+Arrays.toString(targetBuffer));
//			System.err.println("DATA:"+binString(targetBuffer));
//			
//			pr.openPMap(1);
//			System.err.println("template:"+pr.readIntegerUnsigned());
//			System.err.println("34:"+pr.readIntegerUnsigned());
//			System.err.println("52:"+pr.readIntegerUnsigned());
//			System.err.println("131:"+pr.readTextASCII(new StringBuilder()));
//			//pr.openPMap(1);
//			System.err.println("len 146:"+pr.readIntegerUnsigned());
			
			//System.err.println(pr.readIntegerUnsigned());
//			System.err.println(pr.readIntegerUnsigned());
//			System.err.println(pr.readIntegerUnsigned());
//			System.err.println(pr.readIntegerUnsigned());		
//			System.err.println(pr.readIntegerUnsigned());
			//System.err.println(pr.readIntegerUnsigned());
			//System.err.println(pr.readTextASCII(new StringBuilder()));
			
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
	
	private String binString(byte[] targetBuffer) {
		StringBuilder builder = new StringBuilder();
		
		for(byte b:targetBuffer) {
			
			String tmp = Integer.toBinaryString(0xFF&b);
			builder.append(tmp.substring(Math.max(0, tmp.length()-8))).append(" ");
			
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
