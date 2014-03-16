package com.ociweb.jfast.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.xml.parsers.ParserConfigurationException;

import org.openfast.Message;
import org.openfast.MessageInputStream;
import org.openfast.impl.CmeMessageBlockReader;
import org.openfast.template.MessageTemplate;
import org.openfast.template.loader.MessageTemplateLoader;
import org.openfast.template.loader.XMLMessageTemplateLoader;
import org.xml.sax.SAXException;

import com.google.caliper.Benchmark;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.loader.TemplateLoader;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.stream.FASTDynamicReader;
import com.ociweb.jfast.stream.FASTReaderDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;

public class Complex30000Benchmark extends Benchmark {

	FASTInputByteArray fastInput;
	PrimitiveReader primitiveReader;
	FASTDynamicReader dynamicReader;
	FASTRingBuffer queue;
	TemplateCatalog catalog;
	byte[] testData;
	
	public Complex30000Benchmark() {
		FASTInput templateCatalogInput = new FASTInputByteArray(buildRawCatalogData());
		catalog = new TemplateCatalog(new PrimitiveReader(templateCatalogInput));
		
		byte prefixSize = 4;
		catalog.setMessagePreambleSize(prefixSize);	
		
		int maxByteVector = 0;
		catalog.setMaxByteVectorLength(maxByteVector, 0);
				
		int maxTextLength = 14;
		catalog.setMaxTextLength(maxTextLength, 8);
		
		//connect to file		
		URL sourceData = getClass().getResource("/performance/complex30000.dat");
		File fileSource = new File(sourceData.getFile());
			
		try {
			//do not want to time file access so copy file to memory
			testData = new byte[(int) fileSource.length()];
			FileInputStream inputStream = new FileInputStream(fileSource);
			int readBytes = inputStream.read(testData);
			inputStream.close();
			assertEquals(testData.length,readBytes);
			
			fastInput = new FASTInputByteArray(testData);
			primitiveReader = new PrimitiveReader(fastInput);
			FASTReaderDispatch readerDispatch = new FASTReaderDispatch(primitiveReader, 
							                    catalog.dictionaryFactory(), 
							                    3, 
							                    catalog.dictionaryMembers(), 
							                    catalog.getMaxTextLength(),
							                    catalog.getMaxByteVectorLength(),
							                    catalog.getTextGap(),
							                    catalog.getByteVectorGap()); 
			queue = new FASTRingBuffer((byte)8, (byte)7, readerDispatch.textHeap());// TODO: hack test.
			dynamicReader = new FASTDynamicReader(primitiveReader, catalog, queue, readerDispatch);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
	
	public static void main(String[] args) {
		System.err.println("loading...");
		Complex30000Benchmark obj = new Complex30000Benchmark();
		System.err.println("testing...");
		obj.testDecodeComplex30000Two();
		
		System.gc();
		long start = System.nanoTime();
		obj.fastCore(obj.dynamicReader, 1, obj.queue);	
		long duration = System.nanoTime()-start;
		System.err.println("duration:"+duration);
		
//		//obj.openFastTest();
//		long startx = System.nanoTime();
//		int count = 300;
//		obj.timeDecodeComplex30000(count);
//		long durationx = (System.nanoTime()-startx)/count;
//    	System.err.println("done "+durationx+"ns");
				
		
	}
		
	public void testDecodeComplex30000Two() {	
				
		int count = 5;
		int result = 0;
		
		FASTRingBuffer queue = dynamicReader.ringBuffer();
		
		int iter = count;
		while (--iter>=0) {

			double start = System.nanoTime();
			result = fastCore(dynamicReader, result, queue);
			double duration = System.nanoTime()-start;
						
			int ns = (int)(duration);					
			System.err.println("Duration:"+ns+"ns "); //Phrases/Clauses
			
			////////
			//reset the data to run the test again.
			////////
			fastInput.reset();
			primitiveReader.reset();
			dynamicReader.reset(true);
			
		}
		
	}

	private int fastCore(FASTDynamicReader dynamicReader, int result, FASTRingBuffer queue) {
		int flag;
		while (0!=(flag=dynamicReader.hasMore())) {
			if (0!=(flag&0x02)) {
				result|=queue.readInteger(0);//must do some real work or hot-spot may delete this loop.
				queue.dump(); //must dump values in buffer or we will hang when reading.
			}
		}
		return result;
	}
	
	
	public int timeDecodeComplex30000(int reps) {

			
		
		    int result = 0;
			while (--reps>=0) {

				fastCore(dynamicReader, result, queue);
				
				fastInput.reset();
				primitiveReader.reset();
				dynamicReader.reset(false);
				
			}
			return result;
	}
	
//	public int timeDecodeComplex30000ResetOverhead(int reps) {
//
//		FASTRingBuffer queue = dynamicReader.ringBuffer();
//	
//	    int result = 0;
//		while (--reps>=0) {
//	
//			
//			fastInput.reset();
//			primitiveReader.reset();
//			dynamicReader.reset();
//			
//		}
//		return result;
//}
	
	
	public void openFastTest() {
		try {
			MessageTemplateLoader loader = new XMLMessageTemplateLoader();
			URL source = getClass().getResource("/performance/example.xml");
			InputStream aStream = new FileInputStream(new File(source.getFile()));
			MessageTemplate[] templates = loader.load(aStream);
			//System.err.println("templates count "+templates.length);
			//System.err.println(templates[0].getId());
						
		
			source = getClass().getResource("/performance/complex30000.dat");
			File fileSource = new File(source.getFile());
			//do not want to time file access so copy file to memory
			byte[] fileData = new byte[(int) fileSource.length()];
			FileInputStream inputStream = new FileInputStream(fileSource);
			int readBytes = inputStream.read(fileData);
			inputStream.close();
			assertEquals(fileData.length,readBytes);
			
		
			///
			///
			InputStream fastEncodedStream = new ByteArrayInputStream(fileData);
						
			MessageInputStream messageIn = new MessageInputStream(fastEncodedStream);

			//must add support for the 4 byte preamble
			messageIn.setBlockReader(new CmeMessageBlockReader());
			
			messageIn.setTemplateRegistry(loader.getTemplateRegistry());
					
			
			Message msg;
			while (null!=(msg = messageIn.readMessage() )) {
				//System.err.println(msg.getFieldCount());
			}
			
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
