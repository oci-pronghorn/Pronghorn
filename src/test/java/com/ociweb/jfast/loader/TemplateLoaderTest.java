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
import com.ociweb.jfast.stream.FASTDynamicReader;
import com.ociweb.jfast.stream.FASTReaderDispatch;

public class TemplateLoaderTest {

	@Test
	public void buildRawCatalog() {
		byte[] catalogByteArray = buildRawCatalogData();
		
        //reconstruct Catalog object from stream		
		FASTInput input = new FASTInputByteArray(catalogByteArray);
		TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(input));
		
		boolean ok = false;
		long[] script = null;
		try{
			// /performance/example.xml contains 3 templates.
			assertEquals(3, catalog.templatesCount());
			assertEquals(369, catalogByteArray.length);
			
			script = catalog.templateScript(2);
			assertEquals(16, script.length);
			assertEquals(1128, (script[0]>>32));//First Id
			
			//CMD:Group:010000/Close:PMap::010001/9
			//assertEquals(0xC110_0009l,0xFFFFFFFFl&script[script.length-1]);//Last Token
			ok = true;
		} finally {
			if (!ok) {
				System.err.println("Script Details:");
				if (null!=script) {
					System.err.println(convertScriptToString(script));
				}
			}
		}
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
		
		FASTInput templateCatalogInput = new FASTInputByteArray(buildRawCatalogData());
		TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(templateCatalogInput));
		
		int prefixSize = 4;
		catalog.setMessagePrefix(prefixSize);	

		
		//connect to file		
		URL sourceData = getClass().getResource("/performance/complex30000.dat");
		File fileSource = new File(sourceData.getFile());
			
		try {
			//do not want to time file access so copy file to memory
			byte[] fileData = new byte[sourceData.getFile().length()];
			FileInputStream inputStream = new FileInputStream(fileSource);
			int readBytes = inputStream.read(fileData);
			inputStream.close();
			assertEquals(fileData.length,readBytes);
			
			FASTInputByteArray fastInput = new FASTInputByteArray(fileData);
			PrimitiveReader primitiveReader = new PrimitiveReader(fastInput);
			FASTDynamicReader dynamicReader = new FASTDynamicReader(primitiveReader, catalog);
			
			double start=0;
			int warmup = 1000;
			int count = 100; 
			int iter = count+warmup;
			while (--iter>=0) {

				int data = 0; //same id needed for writer construction
				while (0!=(data = dynamicReader.hasMore())) {
					
					//switch on data?
					
					System.err.println("data:"+Integer.toHexString(data));
					
					//int value = dynamicReader.readInt(33);
					//pass dynamic reader into  nextData = dynamicWriter.write(data,dynamicReader); //write can then be stateless
					
				}
				
				fastInput.reset();
				primitiveReader.reset();
				dynamicReader.reset();
				
				if (0==start && iter==count) {
					start = System.nanoTime();
				}
				
			}
			double duration = System.nanoTime()-start;
			System.err.println("Duration:"+(duration/(double)count)+"ns");
			
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
