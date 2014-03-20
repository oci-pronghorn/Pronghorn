package com.ociweb.jfast.loader;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
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
		int[] script = null;
		try{
			// /performance/example.xml contains 3 templates.
			assertEquals(3, catalog.templatesCount());
			assertEquals(467, catalogByteArray.length);
			
			script = catalog.fullScript();
			assertEquals(46, script.length);
			assertEquals(TypeMask.TextASCII, TokenBuilder.extractType(script[0]));//First Token
			
			//CMD:Group:010000/Close:PMap::010001/9
			assertEquals(TypeMask.Group,TokenBuilder.extractType(script[script.length-1]));//Last Token
						
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

	private String convertScriptToString(int[] script) {
		StringBuilder builder = new StringBuilder();
		for(int token:script) {
	
			builder.append(TokenBuilder.tokenToString(token));
			
			builder.append("\n");
		}
		return builder.toString();
	}

	public static void main(String[] args) {
		TemplateLoaderTest tlt = new TemplateLoaderTest();
		tlt.testDecodeComplex30000Two();
	}
	
	public void testDecodeComplex30000Two() {	
		
		FASTInput templateCatalogInput = new FASTInputByteArray(buildRawCatalogData());
		TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(templateCatalogInput));
		
		byte prefixSize = 4;
		catalog.setMessagePreambleSize(prefixSize);	
		
		int maxByteVector = 0;
		catalog.setMaxByteVectorLength(maxByteVector, 0);
				
		int maxTextLength = 14;
		catalog.setMaxTextLength(maxTextLength, 8);
		
		//connect to file		
		URL sourceData = getClass().getResource("/performance/complex30000.dat");

		FASTInputByteArray fastInput = buildInputForTestingByteArray(new File(sourceData.getFile()));
		int totalTestBytes = fastInput.remaining();
		int bufferSize = 4096;
		int pmapDepth = 10;//TODO: Catalog must compute this? 2+(templatePMAP+2)+(max depth + 2 each)
		PrimitiveReader primitiveReader = new PrimitiveReader(bufferSize,fastInput,pmapDepth);
		FASTReaderDispatch readerDispatch = new FASTReaderDispatch(primitiveReader, 
                catalog.dictionaryFactory(), 
                3, 
                catalog.dictionaryMembers(), 
                catalog.getMaxTextLength(),
                catalog.getMaxByteVectorLength(),
                catalog.getTextGap(),
                catalog.getByteVectorGap()); 
		FASTRingBuffer queue = new FASTRingBuffer((byte)8, (byte)7, readerDispatch.textHeap());// TODO: hack test.
		FASTDynamicReader dynamicReader = new FASTDynamicReader(primitiveReader, catalog, queue, readerDispatch);
		
		System.gc();
		
		int count = 5;
		int result = 0;
		
		
		int iter = count;
		while (--iter>=0) {

			double start = System.nanoTime();
			
			int flag;
			while (0!=(flag=dynamicReader.hasMore())) {
				if (0!=(flag&TemplateCatalog.END_OF_MESSAGE)) {
					result|=queue.readInteger(0);//must do some real work or hot-spot may delete this loop.
					queue.dump(); //must dump values in buffer or we will hang when reading.
				}
			}				

			double duration = System.nanoTime()-start;
			
			
			int ns = (int)(duration/count);
			float nsPerByte = (ns/(float)totalTestBytes);
			int mbps = (int)((1000l*totalTestBytes*8l)/ns);
					
			System.err.println("Duration:"+ns+"ns "+
					           " "+nsPerByte+"nspB "+
					           " "+mbps+"mbps "+
					           " Bytes:"+totalTestBytes); //Phrases/Clauses
			
			////////
			//reset the data to run the test again.
			////////
			fastInput.reset();
			primitiveReader.reset();
			dynamicReader.reset(true);
			
		}
		
	}
	
	
	@Test
	public void testDecodeComplex30000() {	
		
		FASTInput templateCatalogInput = new FASTInputByteArray(buildRawCatalogData());
		TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(templateCatalogInput));
		
		//values which need to be set client side and are not in the template.
		catalog.setMessagePreambleSize((byte)4);	
		catalog.setMaxByteVectorLength(0, 0);//byte vectors are unused
		catalog.setMaxTextLength(14, 8);
		
		//connect to file		
		URL sourceData = getClass().getResource("/performance/complex30000.dat");
		File sourceDataFile = new File(sourceData.getFile());
		long totalTestBytes = sourceDataFile.length();
		
		
		//FASTInputByteArray fastInput = buildInputForTestingByteArray(sourceDataFile);

		//New memory mapped solution. No need to cache because we warm up and OS already has it.
		FASTInputByteBuffer fastInput = buildInputForTestingByteBuffer(sourceDataFile);

		
		int bufferSize = 4096;
		int pmapDepth = 10;//TODO: Catalog must compute this? 2+(templatePMAP+2)+(max depth + 2 each)
		PrimitiveReader primitiveReader = new PrimitiveReader(bufferSize,fastInput,pmapDepth);
		FASTReaderDispatch readerDispatch = new FASTReaderDispatch(
													primitiveReader, 
									                catalog.dictionaryFactory(), 
									                3, 
									                catalog.dictionaryMembers(), 
									                catalog.getMaxTextLength(),
									                catalog.getMaxByteVectorLength(),
									                catalog.getTextGap(),
									                catalog.getByteVectorGap()); 
		FASTRingBuffer queue = new FASTRingBuffer((byte)8, (byte)7, readerDispatch.textHeap());// TODO: hack test.
		FASTDynamicReader dynamicReader = new FASTDynamicReader(primitiveReader, catalog, queue, readerDispatch);
		
		System.gc();
		
		int warmup = 90;//set much larger for profiler
		int count = 5;
		int result = 0;
		int[] fullScript = catalog.scriptTokens;
		
		int msgs = 0;
		int grps = 0;
		long queuedBytes = 0;
		int iter = warmup;
		while (--iter>=0) {
			msgs = 0;
			grps = 0;
			int flag = 0; //same id needed for writer construction
			while (0!=(flag = dynamicReader.hasMore())) {
				//New flags 
				//0000  eof
				//0001  has sequence group to read (may be combined with end of message)
				//0010  has message to read
				//neg   unable to write to ring buffer
								
				//consumer code can stop only at end of message if desired or for 
				//lower latency can stop at the end of every sequence entry.
				//the call to hasMore is non-blocking with respect to the consumer and 
				//will return negative value if the ring buffer is full but it will 
				//spin lock if input stream is not ready.
				//
				
				if (0!=(flag&TemplateCatalog.END_OF_MESSAGE)) {
					msgs++;
					//this is a template message. 
					int bufferIdx = 0;
					int templateId = queue.readInteger(bufferIdx);
					bufferIdx+=1;//point to first field
					assertTrue("found "+templateId,1==templateId || 2==templateId || 99==templateId);
					
					int i = catalog.getTemplateStartIdx(templateId);
					int limit = catalog.getTemplateLimitIdx(templateId);
					//System.err.println("new templateId "+templateId);
					while (i<limit) {
						int token = fullScript[i++];	
						//System.err.println("xxx:"+bufferIdx+" "+TokenBuilder.tokenToString(token));
						
						if (isText(token)) {
							queuedBytes += (4*queue.getCharLength(bufferIdx));
														
						}
						
						//find the next index after this token.
						bufferIdx += stepSizeInRingBuffer(token);
						
					}
					queuedBytes += bufferIdx;//ring buffer bytes, NOT full string byteVector data.
					
					//must dump values in buffer or we will hang when reading.
					//only dump at end of template not end of sequence.
					//the removePosition must remain at the beginning until message is complete.					
					queue.dump(); 
				} 
				grps++;
				
			}		
			fastInput.reset();
			primitiveReader.reset();
			dynamicReader.reset(true);
		}
		
		iter = count;
		while (--iter>=0) {

			double start = System.nanoTime();
			
			int flag;
			while (0!=(flag=dynamicReader.hasMore())) {
				if (0!=(flag&TemplateCatalog.END_OF_MESSAGE)) {
					result|=queue.readInteger(0);//must do some real work or hot-spot may delete this loop.
					queue.dump(); //must dump values in buffer or we will hang when reading.
				}
			}				

			double duration = System.nanoTime()-start;
			
			
			int ns = (int)duration;
			float mmsgPerSec = (msgs*(float)1000l/ns);
			float nsPerByte = (ns/(float)totalTestBytes);
			int mbps = (int)((1000l*totalTestBytes*8l)/ns);
					
			System.err.println("Duration:"+ns+"ns "+
					           " "+mmsgPerSec+"MM/s "+
					           " "+nsPerByte+"nspB "+
					           " "+mbps+"mbps "+
					           " In:"+totalTestBytes+
					           " Out:"+queuedBytes+" pct "+(totalTestBytes/(float)queuedBytes)+
					           " Messages:"+msgs+
   			           		   " Groups:"+grps); //Phrases/Clauses
			
			////////
			//reset the data to run the test again.
			////////
			fastInput.reset();
			primitiveReader.reset();
			dynamicReader.reset(true);
			
		}
		assertTrue(result!=0);	
		
	}

	private boolean isText(int token) {
		return 0x08==(0x1F& (token>>>TokenBuilder.SHIFT_TYPE) );
	}

	private int stepSizeInRingBuffer(int token) {
		int stepSize = 0;
		if (0==(token&(16<<TokenBuilder.SHIFT_TYPE))) {
			//0????
			if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
				//00???
				if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
					//int
					stepSize = 1;
				} else {
					//long
					stepSize = 2;
				}
			} else {
				//01???
				if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
					//int for text (takes up 2 slots)
					stepSize = 2;			
				} else {
					//011??
					if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
						//0110? Decimal and DecimalOptional
						stepSize = 3;
					} else {
						//int for bytes
						stepSize = 0;//BYTES ARE NOT IMPLEMENTED YET BUT WILL BE 2;
					}
				}
			}
		} else {
			if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
				//10???
				if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
					//100??
					//Group Type, no others defined so no need to keep checking
					stepSize = 0;
				} else {
					//101??
					//Length Type, no others defined so no need to keep checking
					//Only happens once before a node sequence so push it on the count stack
					stepSize = 1;
				}
			} else {
				//11???
				//Dictionary Type, no others defined so no need to keep checking
				stepSize = 0;
			}
		}
		
		return stepSize;
	}

	private FASTInputByteBuffer buildInputForTestingByteBuffer(File sourceDataFile) {
		long totalTestBytes = sourceDataFile.length();
		FASTInputByteBuffer fastInput = null;
		try {
			FileChannel fc = new RandomAccessFile(sourceDataFile, "r").getChannel();
			MappedByteBuffer mem =fc.map(FileChannel.MapMode.READ_ONLY, 0, totalTestBytes);
            fastInput = new FASTInputByteBuffer(mem);
			fc.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fastInput;
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
//		int totalTestBytes = fastInput.remaining();
//		PrimitiveReader primitiveReader = new PrimitiveReader(fastInput);
//		FASTDynamicReader dynamicReader = new FASTDynamicReader(primitiveReader, catalog);
//		
//		byte[] targetBuffer = new byte[(int)fileSource.length()];
//		FASTOutputByteArray fastOutput = new FASTOutputByteArray(targetBuffer);
//		PrimitiveWriter primitiveWriter = new PrimitiveWriter(fastOutput);
//		FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(primitiveWriter, catalog);
//		
//		System.gc();
//		
//		int warmup = 3;//set much larger for profiler
//		int count = 5;
//		
//		FASTRingBuffer queue = dynamicReader.ringBuffer();
//		
//		int msgs = 0;
//		int grps = 0;
//		int iter = warmup;
//		while (--iter>=0) {
//			msgs = 0;
//			grps = 0;
//			int data = 0; //same id needed for writer construction
//			while (0!=(data = dynamicReader.hasMore())) {
//				dynamicWriter.write();
//				if (0!=(data&END_OF_MESSAGE)) {
//					msgs++;
//				}
//				grps++;
//			}		
//			fastInput.reset();
//			primitiveReader.reset();
//			dynamicReader.reset(true);
//		}
//		
//		iter = count;
//		while (--iter>=0) {
//
//			double start = System.nanoTime();
//				while (0!=dynamicReader.hasMore()) {
//					dynamicWriter.write(queue);
//				}
//				
//			double duration = System.nanoTime()-start;
//			int ns = (int)duration;
//			float mmsgPerSec = (msgs*(float)1000l/ns);
//			float nsPerByte = (ns/(float)totalTestBytes);
//			int mbps = (int)((1000l*totalTestBytes*8l)/ns);
//					
//			System.err.println("Duration:"+ns+"ns "+
//					           " "+mmsgPerSec+"MM/s "+
//					           " "+nsPerByte+"nspB "+
//					           " "+mbps+"mbps "+
//					           " Bytes:"+totalTestBytes+
//					           " Messages:"+msgs+
//   			           		   " Groups:"+grps); //Phrases/Clauses
//			
//			//TODO: confirm generated bytes match parsed.
//			
//			////////
//			//reset the data to run the test again.
//			////////
//			fastInput.reset();
//			primitiveReader.reset();
//			dynamicReader.reset(true);
//			
//		}	
//				
//	}
	
	private FASTInputByteArray buildInputForTestingByteArray(File fileSource) {
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
