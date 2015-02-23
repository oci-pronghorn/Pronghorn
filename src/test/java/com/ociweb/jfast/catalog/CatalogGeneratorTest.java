package com.ociweb.jfast.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.jfast.FAST;
import com.ociweb.jfast.catalog.generator.CatalogGenerator;
import com.ociweb.jfast.catalog.generator.TemplateGenerator;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateHandler;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.RingWalker;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TypeMask;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTReaderReactor;


public class CatalogGeneratorTest {
    
    int[] numericTypes = new int[] {
            TypeMask.IntegerUnsigned,
            TypeMask.IntegerUnsignedOptional,
            TypeMask.IntegerSigned,
            TypeMask.IntegerSignedOptional,
            TypeMask.LongUnsigned,
            TypeMask.LongUnsignedOptional,
            TypeMask.LongSigned,
            TypeMask.LongSignedOptional,
            TypeMask.Decimal,
            TypeMask.DecimalOptional
    };
    
    int[] numericOps = new int[] {
    		OperatorMask.Field_Copy,   
    		OperatorMask.Field_Delta,
    		OperatorMask.Field_None, 
    		OperatorMask.Field_Constant,
            OperatorMask.Field_Default, 
            OperatorMask.Field_Increment,            
    };
    
    int[] textByteOps = new int[] {
            OperatorMask.Field_Constant,
            OperatorMask.Field_Copy,
            OperatorMask.Field_Default,
            OperatorMask.Field_Delta,
            OperatorMask.Field_Increment,            
            OperatorMask.Field_None,
            OperatorMask.Field_Tail
    };
    
    int[] textTypes = new int[] {
            TypeMask.TextASCII,
            TypeMask.TextASCIIOptional,
            TypeMask.TextUTF8,
            TypeMask.TextUTF8Optional
    };
    
    int[] byteTypes = new int[] {
            TypeMask.ByteArray,
            TypeMask.ByteArrayOptional
    };    
    
   
    private final int writeBuffer=4096;
    private int testRecordCount = 3;//100;//100000; //testing enough to get repeatable results
    
    private static final int testTemplateId = 2;
    private static final int testMessageIdx = 0;
    
    
    List<String>  numericCatalogXML;
    List<byte[]>  numericCatalogs;
    List<Integer> numericFieldCounts;
    List<Integer> numericFieldTypes;
    List<Integer> numericFieldOperators;
    
    List<byte[]> textCatalogs;
    List<Integer> textFieldCounts;
    List<Integer> textFieldTypes;
    List<Integer> textFieldOperators;
    
    
    @Before
    public void buildCatalogs() {
        
    	numericCatalogXML = new ArrayList<String>();
        numericCatalogs = new ArrayList<byte[]>();
        numericFieldCounts = new ArrayList<Integer>();
        numericFieldTypes = new ArrayList<Integer>();
        numericFieldOperators = new ArrayList<Integer>();
        
        textCatalogs = new ArrayList<byte[]>();
        textFieldCounts = new ArrayList<Integer>();
        textFieldTypes = new ArrayList<Integer>();
        textFieldOperators = new ArrayList<Integer>();
        
        String name = "testTemplate";
        
        boolean reset = false;
        String dictionary = null;
        boolean fieldPresence = false;     
        String fieldInitial = "10";        
        int totalFields = 10;//400;//1024;  //at 4 bytes per int for 4K message size test                              
        
        int p = numericOps.length;
        while (--p>=0) {            
            int fieldOperator = numericOps[p];            
            int t = numericTypes.length;
            while (--t>=0) {               
                int fieldType = numericTypes[t];
                int fieldCount = 1; 
                while (fieldCount<totalFields) {     
                	StringBuilder templateXML = new StringBuilder();
                    byte[] catBytes = buildCatBytes(name, testTemplateId, reset, dictionary, fieldPresence, fieldInitial, fieldOperator, fieldType, fieldCount, templateXML);  
                    
                    
                    TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);                    
                    assertEquals(1, catalog.templatesCount());
                    
                    int expectedScriptLength = 2+fieldCount;
                    if (fieldType == TypeMask.Decimal || fieldType == TypeMask.DecimalOptional) {
                        expectedScriptLength +=fieldCount;
                    }
                    assertEquals(expectedScriptLength,catalog.getScriptTokens().length);
                
                    numericCatalogXML.add(templateXML.toString());
                    numericCatalogs.add(catBytes);
                    numericFieldCounts.add(new Integer(fieldCount));
                    numericFieldTypes.add(new Integer(fieldType));
                    numericFieldOperators.add(new Integer(fieldOperator));
                    
                    if (fieldCount<10) {
                        fieldCount+=1;
                    } else if (fieldCount<100) {
                        fieldCount+=10;
                    } else {
                        fieldCount+=100;//by steps of 100, 
                    }
                }
            }            
        } 
        
        //cleanup now that our test data is built so it will not run later
        System.gc();        
        
    }
    
    
    @Test
    public void numericFieldTest() {
        
        AtomicLong totalWrittenCount = new AtomicLong();
        int i = numericCatalogs.size();
        System.out.println("testing "+i+" numeric configurations");
        while (--i>=0) {
            testEncoding(numericFieldOperators.get(i).intValue(), 
                         numericFieldTypes.get(i).intValue(), 
                         numericFieldCounts.get(i).intValue(), 
                         numericCatalogs.get(i),
                         totalWrittenCount, 
                         numericCatalogXML.get(i),
                         numericCatalogs.size()-i);
        }
        System.err.println("totalWritten:"+totalWrittenCount.longValue());
            
    }
    
//    @Test
//    public void textFieldTest() {
//        
//        AtomicLong totalWrittenCount = new AtomicLong();
//        int i = textCatalogs.size();
//        System.out.println("testing "+i+" text configurations");
//        while (--i>=0) {
//            testEncoding(textFieldOperators.get(i).intValue(), 
//            			 textFieldTypes.get(i).intValue(), 
//            			 textFieldCounts.get(i).intValue(), 
//            			 textCatalogs.get(i),
//                         totalWrittenCount);
//        }
//        System.err.println("totalWritten:"+totalWrittenCount.longValue());
//            
//    }


    int lastOp = -1;
    int lastType = -1;
    int lastFieldCount = -1;
    

    public void testEncoding(int fieldOperator, int fieldType, int fieldCount, byte[] catBytes, AtomicLong totalWritten, String catalogXML, int ordinal) {
        int type = fieldType;
        int operation = fieldOperator;
               
        TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
                
        assertEquals(1, catalog.templatesCount());
        
        FASTClassLoader.deleteFiles();
        
        catalog.clientConfig();
		catalog.clientConfig();
		RingBuffers ringBuffers= RingBuffers.buildNoFanRingBuffers(new RingBuffer(new RingBufferConfig((byte)15, (byte)7, catalog.ringByteConstants(), catalog.getFROM())));
                
        //FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriter(catBytes); //compiles new encoder         
        FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriterDebug(catBytes);
        

        boolean debug = false;
        if (operation!=lastOp) {
            lastOp = operation;
            
            System.err.println(this.getClass()+" using "+writerDispatch.getClass().getSimpleName()+" testing "+OperatorMask.methodOperatorName[operation]);
            
        }
        
        if (type!=lastType) {
            lastType = type;
            if (debug) {
            	System.err.println("type:"+TypeMask.methodTypeName[type]+TypeMask.methodTypeSuffix[type]);  
            }
        }

        
                        
        //If this test is going to both encode then decode to test both parts of the process this
        //buffer must be very large in order to hold all the possible permutations
        byte[] buffer = new byte[1<<24];
        FASTOutput fastOutput = new FASTOutputByteArray(buffer );
        PrimitiveWriter writer = new PrimitiveWriter(writeBuffer, fastOutput, true);

        RingBuffer ringBuffer = RingBuffers.get(ringBuffers,0);
        FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(writer, ringBuffer, writerDispatch);
             
        //populate ring buffer with the new records to be written.
        
        float millionPerSecond = timeEncoding(fieldType, fieldCount, ringBuffer, dynamicWriter)/1000000f;        
        PrimitiveWriter.flush(writer);
        long bytesWritten = PrimitiveWriter.totalWritten(writer);
        assertEquals(0, PrimitiveWriter.bytesReadyToWrite(writer));
        
        //Use as bases for building single giant test file with test values provided, in ascii?
        totalWritten.addAndGet(PrimitiveWriter.totalWritten(writer));
             
        //TODO: D, write to flat file to produce google chart.
       //System.err.println(TypeMask.xmlTypeName[fieldType]+" "+OperatorMask.xmlOperatorName[fieldOperator]+" fields: "+ fieldCount+" latency:"+nsLatency+"ns total mil per second "+millionPerSecond);
        //System.err.println("bytes written:"+bytesWritten);
        
        //This visual check confirms that the write
//        int limit = (int) Math.min(bytesWritten, 10);
//        int q = 0;
//        while (q<limit) {
//        	//template pmap
//        	//template id of zero
//        	//data 0, 1 ,63, 64, 65
//        	System.err.println(q+"   "+byteString(buffer[q]));
//        	q++;
//        	
//        }
        
        //delta
//        0   11000000   PMap we need template id
//        1   10000010   The template id of 2
//        2   00000111   int32 field1000 delta
//        3   01111111   int32 field1000
//        4   01111111   int32 field1000
//        5   01111111   int32 field1000
//        6   11110100   int32 field1000
//        7   00000000   int32 field1001 delta
//        8   01111111   int32 field1001
//        9   01111111   int32 field1001
        
        //constant
//        0   11000000   PMap we need template id
//        1   10000010   The template id of 2
//        2   11000000   PMap we need template id
//        3   10000010   The template id of 2
//        4   11000000
//        5   10000010
        
        //none
//        0   11000000  PMap we need template id
//        1   10000010  The template id of 2
//        2   00000111  int32 field1000 none
//        3   01111111  int32 field1000
//        4   01111111  int32 field1000
//        5   01111111  int32 field1000
//        6   11111110  int32 field1000
//        7   00000001  int32 field1001 none
//        8   00000000  int32 field1001
//        9   00000000  int32 field1001
        
        //Default  with 9 defaults each needing pmap bit set to 1
//        0   01111111  Top bit set for tempate, following 9 are for default fields
//        1   11110000  end of pmap has trailing zeros
//        2   10000010  The template id of 2
//        3   00000111  int32 field1000 not default
//        4   01111111  int32 field1000
//        5   01111111  int32 field1000
//        6   01111111  int32 field1000
//        7   11111110  int32 field1000
//        8   00000001  int32 field1001 not default
//        9   00000000  int32 field1001
        

        
        FASTInput fastInput = new FASTInputByteArray(buffer, (int)bytesWritten);
        

        FASTReaderReactor reactor = FAST.inputReactorDebug(fastInput, catBytes, ringBuffers);
        //FASTReaderReactor reactor = FAST.inputReactor(fastInput, catBytes);
        

        RingBuffer[] buffers = reactor.ringBuffers();
        int buffersCount = buffers.length;
        try {
	        int j = testRecordCount;
	        while (j>0 && FASTReaderReactor.pump(reactor)>=0) { //continue if there is no room or if a fragment is read.
	        	int k = buffersCount;
	        	while (j>0 && --k>=0) {
	        		//System.err.println(j);
	        		if (RingReader.tryReadFragment(buffers[k])) {
	        			RingWalker r1 = buffers[k].ringWalker;
						assertTrue(RingReader.isNewMessage(r1));
						RingWalker r = buffers[k].ringWalker;
	        			assertEquals(testMessageIdx, RingReader.getMsgIdx(r));
	        			
	        			//TODO: B, add test in here to confirm the values match
	        			
	        			j--;
	        		}        		
	        	}       	
	//        	System.err.println("end of message "+ reactor.reader.position);
	        }
	        //confirm that the internal stacks have gone back down to zero
	        assertEquals(0,writer.safetyStackDepth);
	        assertEquals(0,writer.flushSkipsIdxLimit);
	        
        } catch (Exception ex) {
        	//TODO: the template ID appears to be missing for the new message!!
            System.err.println(catalogXML);
        	
            //dump exactly what was written
	        int limit = (int) Math.min(bytesWritten, 55);
	        int q = 0;
	        while (q<limit) {
	          	System.err.println(q+"   "+byteString(buffer[q]));
	          	q++;
	          	
	        }
            
        	throw new RuntimeException(ex);
        }

    }


	private String byteString(int value) {
		String tmp = "00000000"+Integer.toBinaryString(value);
		return tmp.substring(tmp.length()-8, tmp.length());
	}

    //TODO: B, need to review all misconfigured error messages to ensure that they are helpful and point in the right direction.
    

    private float timeEncoding(int fieldType, final int fieldCount, RingBuffer ringBuffer, FASTDynamicWriter dynamicWriter) {
       
//    	System.err.println("field Count:"+fieldCount);
//    	FieldReferenceOffsetManager.printScript("timeEncoding", ringBuffer.consumerData.from);
//    	System.err.println(Arrays.toString(ringBuffer.consumerData.from.fragScriptSize));
    	    	
    	int i = testRecordCount;
        int d;
        switch(fieldType) {
            case TypeMask.IntegerUnsigned:
            case TypeMask.IntegerUnsignedOptional:
            case TypeMask.IntegerSigned:
            case TypeMask.IntegerSignedOptional:
                {                 	
                    long start = System.nanoTime();
                    
                    d = ReaderWriterPrimitiveTest.unsignedIntData.length;
                    while (--i>=0) {
                        RingBuffer.addMsgIdx(ringBuffer, testMessageIdx);
                        
                        int j = fieldCount;
                        while (--j>=0) {
                            RingBuffer.addValue(ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos, ReaderWriterPrimitiveTest.unsignedIntData[--d]);
                            
                            if (0 == d) {
                                d = ReaderWriterPrimitiveTest.unsignedIntData.length;
                            }
                            
                        }
                        RingBuffer.publishWrites(ringBuffer);
                        
                        if (RingReader.tryReadFragment(ringBuffer)) {//without move next we get no stats.
                        	
                        	RingReader.getMsgIdx(ringBuffer);
                        	
                            FASTDynamicWriter.write(dynamicWriter);
                        }
                    }
                    long duration = System.nanoTime()-start;
                    return 1000000000f*testRecordCount/duration;
                }
            case TypeMask.LongUnsigned:
            case TypeMask.LongUnsignedOptional:
            case TypeMask.LongSigned:
            case TypeMask.LongSignedOptional:
                { 
                    long start = System.nanoTime();
                    
                    d = ReaderWriterPrimitiveTest.unsignedLongData.length;
                  
                    while (--i>=0) {
                    	RingBuffer.addMsgIdx(ringBuffer, testMessageIdx);
                    	 
                        int j = fieldCount;
                        while (--j>=0) {
                            RingBuffer.addLongValue(ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos, ReaderWriterPrimitiveTest.unsignedLongData[--d]);
                            if (0==d) {
                                d = ReaderWriterPrimitiveTest.unsignedLongData.length;
                            }
                        }
                        RingBuffer.publishWrites(ringBuffer);
                        if (RingReader.tryReadFragment(ringBuffer)) {//without move next we get no stats.
                            FASTDynamicWriter.write(dynamicWriter);
                        }
                    }
                    long duration = System.nanoTime()-start;
                    return 1000000000f*testRecordCount/duration;
                }
            case TypeMask.Decimal:
            case TypeMask.DecimalOptional:
                {
                    long start = System.nanoTime();
                    
                    int exponent = 2;
                    d = ReaderWriterPrimitiveTest.unsignedLongData.length;
          
                    while (--i>=0) {
                        RingBuffer.addMsgIdx(ringBuffer, testMessageIdx);
                        int j = fieldCount;
                        while (--j>=0) {
                            RingBuffer.addValues(ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos, exponent, ReaderWriterPrimitiveTest.unsignedLongData[--d]);
                            if (0==d) {
                                d = ReaderWriterPrimitiveTest.unsignedLongData.length;
                            }
                        }
                        RingBuffer.publishWrites(ringBuffer);
                        if (RingReader.tryReadFragment(ringBuffer)) {//without move next we get no stats.
                            FASTDynamicWriter.write(dynamicWriter);
                        }
                    }
                    long duration = System.nanoTime()-start;
                    return 1000000000f*testRecordCount/duration;
                } 
            case TypeMask.TextASCII:
            case TypeMask.TextASCIIOptional:
            case TypeMask.TextUTF8:
            case TypeMask.TextUTF8Optional:
                {
                    long start = System.nanoTime();
                    
                    int exponent = 2;
                    d = ReaderWriterPrimitiveTest.stringData.length;
      
                    while (--i>=0) {
                        RingBuffer.addMsgIdx(ringBuffer, testMessageIdx);
                        int j = fieldCount;
                        while (--j>=0) {
                            //TODO: B, this test is not using UTF8 encoding for the UTF8 type mask!!!! this is only ASCII enoding always.
                            byte[] source = ReaderWriterPrimitiveTest.stringDataBytes[--d];
							RingBuffer.addByteArray(source, 0, source.length, ringBuffer);
                            if (0==d) {
                                d = ReaderWriterPrimitiveTest.stringData.length;
                            }
                        }
                        RingBuffer.publishWrites(ringBuffer);
                        if (RingReader.tryReadFragment(ringBuffer)) {//without move next we get no stats.
                            FASTDynamicWriter.write(dynamicWriter);
                        }
                    }
                    long duration = System.nanoTime()-start;
                    return 1000000000f*testRecordCount/duration;
                    
                }
                
        
        }
        return 0;
    }



    private byte[] buildCatBytes(String name, int id, boolean reset, String dictionary, boolean fieldPresence,
            String fieldInitial, int fieldOperator, int fieldType, int f, StringBuilder builder) {
        
        int fieldId = 1000;
        
        CatalogGenerator cg = new CatalogGenerator();
        TemplateGenerator template = cg.addTemplate(name, id, reset, dictionary);            
        
        while (--f>=0) {
            String fieldName = "field"+fieldId;
            template.addField(fieldName, fieldId++, fieldPresence, fieldType, fieldOperator, fieldInitial);        
        }
        
        
		try {
			builder = (StringBuilder) cg.appendTo("", builder);
			boolean debug = false;
			if (debug) {
				System.err.println(builder);
			}
			
			
			ClientConfig clientConfig = new ClientConfig(21,19);  //keep bits small or the test will take a very long time to run.              
			byte[] catBytes = convertTemplateToCatBytes(builder, clientConfig);
			return catBytes;
		} catch (IOException e) {
			throw new FASTException(e);
		}        
   
    }



    public static byte[] convertTemplateToCatBytes(StringBuilder builder, ClientConfig clientConfig) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            GZIPOutputStream gZipOutputStream = new GZIPOutputStream(baos);
            FASTOutput output = new FASTOutputStream(gZipOutputStream);
            
            SAXParserFactory spfac = SAXParserFactory.newInstance();
            SAXParser sp = spfac.newSAXParser();
            InputStream stream = new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8));           
            
            TemplateHandler handler = new TemplateHandler(output, clientConfig);            
            sp.parse(stream, handler);
    
            handler.postProcessing(clientConfig.getBytesGap(), clientConfig.getBytesLength());
            gZipOutputStream.close();            
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        byte[] catBytes = baos.toByteArray();
        return catBytes;
    }
    
    
    
    public static byte[] buildRawCatalogData(ClientConfig clientConfig) {
        //this example uses the preamble feature
        clientConfig.setPreableBytes((short)4);

        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            TemplateLoader.buildCatalog(catalogBuffer, "/performance/example.xml", clientConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertTrue("Catalog must be built.", catalogBuffer.size() > 0);

        byte[] catalogByteArray = catalogBuffer.toByteArray();
        return catalogByteArray;
    }
    
    
    

}
