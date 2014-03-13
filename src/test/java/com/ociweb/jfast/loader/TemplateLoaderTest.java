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
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.stream.FASTDynamicReader;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTReaderDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;

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
			assertEquals(469, catalogByteArray.length);
			
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
	
//TODO: build FAST debugger that can break data without template on stop bit and provide multiple possible interpretations.
	
	// Runs very well with these JVM arguments
	// -XX:CompileThreshold=64 -XX:+AlwaysPreTouch -XX:+UseNUMA -XX:+AggressiveOpts -XX:MaxInlineLevel=20
	// ?? -XX:+UseFPUForSpilling -XX:InlineSmallCode=65536
			
	@Test
	public void testDecodeComplex30000() {	
		
		FASTInput templateCatalogInput = new FASTInputByteArray(buildRawCatalogData());
		TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(templateCatalogInput));
		
		byte prefixSize = 4;
		catalog.setMessagePrefix(prefixSize);	
		
		int maxByteVector = 0;
		catalog.setMaxByteVectorLength(maxByteVector);
				
		int maxTextLength = 14;
		catalog.setMaxTextLength(maxTextLength);
		
		//connect to file		
		URL sourceData = getClass().getResource("/performance/complex30000.dat");

		FASTInputByteArray fastInput = buildInputForTesting(new File(sourceData.getFile()));
		PrimitiveReader primitiveReader = new PrimitiveReader(fastInput);
		FASTDynamicReader dynamicReader = new FASTDynamicReader(primitiveReader, catalog);
		
		System.gc();
		
		double start=0;
		int warmup = 120;//120;//set much larger for profiler
		int count = 20;//20;
		int iter = count+warmup;
		int result = 0;
		FASTRingBuffer queue = dynamicReader.ringBuffer();
		
		while (--iter>=0) {

			int data = 0; //same id needed for writer construction
			while (0!=(data = dynamicReader.hasMore())) {
				//TODO: only jump to next record instead of dumping all
				queue.dump(); //must dump values in buffer or we will hang when reading.
				result |=data;
			}
			
			fastInput.reset();
			primitiveReader.reset();
			if (0==start) {
				//System.err.println(warmup-(count+warmup-iter)+" "+dynamicReader.messageCount());
				if (iter==count) {
					start = System.nanoTime();
				}
			}
			dynamicReader.reset();
			
		}
		double duration = System.nanoTime()-start;
		int ns = (int)(duration/count);
		System.err.println("Avg duration:"+ns+"ns");
		assertTrue(result!=0);	
		
	}

//	@Test
//	public void testDecodeEncodeComplex30000() {	
//		FASTInput templateCatalogInput = new FASTInputByteArray(buildRawCatalogData());
//		TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(templateCatalogInput));
//		
//		byte prefixSize = 4;
//		catalog.setMessagePrefix(prefixSize);	
//		
//		//connect to file		
//		URL sourceData = getClass().getResource("/performance/complex30000.dat");
//
//		File fileSource = new File(sourceData.getFile());
//		FASTInputByteArray fastInput = buildInputForTesting(fileSource);
//		PrimitiveReader primitiveReader = new PrimitiveReader(fastInput);
//		FASTDynamicReader dynamicReader = new FASTDynamicReader(primitiveReader, catalog);
//		
//		byte[] targetBuffer = new byte[(int)fileSource.length()];
//		FASTOutputByteArray fastOutput = new FASTOutputByteArray(targetBuffer);
//		PrimitiveWriter primitiveWriter = new PrimitiveWriter(fastOutput);
//		FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(primitiveWriter, catalog);
//		
//		
//		
//		System.gc();
//		
//		double start=0;
//		int warmup = 120;//set much larger for profiler
//		int count = 20;
//		int iter = count+warmup;
//		FASTRingBuffer queue = dynamicReader.ringBuffer();
//		
//		while (--iter>=0) {
//
//			int data = 0; //same id needed for writer construction
//			while (0!=(data = dynamicReader.hasMore())) {	
//				queue.dump();//Hack for now
//				dynamicWriter.write(data, queue);			
//			}
//			
//			fastInput.reset();
//			primitiveReader.reset();
//			if (0==start) {
//				//System.err.println(warmup-(count+warmup-iter)+" "+dynamicReader.messageCount());
//				if (iter==count) {
//					start = System.nanoTime();
//				}
//			}
//			dynamicReader.reset();
//			
//		}
//		double duration = System.nanoTime()-start;
//		int ns = (int)(duration/count);
//		System.err.println("Avg duration:"+ns+"ns");
//				
//	}
	
	private FASTInputByteArray buildInputForTesting(File fileSource) {
		byte[] fileData = null;
		try {
			//do not want to time file access so copy file to memory
			fileData = new byte[(int) fileSource.length()];
			FileInputStream inputStream = new FileInputStream(fileSource);
			int readBytes = inputStream.read(fileData);
			inputStream.close();
			assertEquals(fileData.length,readBytes);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
			FASTInputByteArray fastInput = new FASTInputByteArray(fileData);
		return fastInput;
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
