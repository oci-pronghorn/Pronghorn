package com.ociweb.jfast.catalog;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
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
import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.jfast.FAST;
import com.ociweb.jfast.catalog.generator.CatalogGenerator;
import com.ociweb.jfast.catalog.generator.TemplateGenerator;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.FieldReferenceOffsetManager;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateHandler;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTInputReactor;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferConsumer;
import com.ociweb.jfast.stream.FASTRingBufferWriter;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;
import com.ociweb.jfast.stream.RingBuffers;
import com.ociweb.jfast.util.Stats;


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
        //    OperatorMask.Field_Copy,
        //    OperatorMask.Field_Default,
        //    OperatorMask.Field_Increment,            
        //    OperatorMask.Field_Delta,
            OperatorMask.Field_None, 
            OperatorMask.Field_Constant,
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
    
    List<byte[]> numericCatalogs;
    List<Integer> numericFieldCounts;
    List<Integer> numericFieldTypes;
    List<Integer> numericFieldOperators;
    
    @Before
    public void buildCatalogs() {
        
        numericCatalogs = new ArrayList<byte[]>();
        numericFieldCounts = new ArrayList<Integer>();
        numericFieldTypes = new ArrayList<Integer>();
        numericFieldOperators = new ArrayList<Integer>();
        
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
                    byte[] catBytes = buildCatBytes(name, testTemplateId, reset, dictionary, fieldPresence, fieldInitial, fieldOperator, fieldType, fieldCount);  
                    
                    
                    TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);                    
                    assertEquals(1, catalog.templatesCount());
                    
                    int expectedScriptLength = 2+fieldCount;
                    if (fieldType == TypeMask.Decimal || fieldType == TypeMask.DecimalOptional) {
                        expectedScriptLength +=fieldCount;
                    }
                    assertEquals(expectedScriptLength,catalog.getScriptTokens().length);
                
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
                         totalWrittenCount);
        }
        System.err.println("totalWritten:"+totalWrittenCount.longValue());
            
    }


    int lastOp = -1;
    int lastType = -1;
    int lastFieldCount = -1;
    

    public void testEncoding(int fieldOperator, int fieldType, int fieldCount, byte[] catBytes, AtomicLong totalWritten) {
        int type = fieldType;
        int operation = fieldOperator;
               
        TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
        
        
        if (operation!=lastOp) {
            lastOp = operation;
            System.err.println();
            System.err.println("operation:"+OperatorMask.xmlOperatorName[operation]);
        }
        
        if (type!=lastType) {
            lastType = type;
            System.err.println("type:"+TypeMask.methodTypeName[type]+TypeMask.methodTypeSuffix[type]);           
        }

        assertEquals(1, catalog.templatesCount());
        
        FASTClassLoader.deleteFiles();
        FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriter(catBytes); //compiles new encoder
                        
        //If this test is going to both encode then decode to test both parts of the process this
        //buffer must be very large in order to hold all the possible permutations
        byte[] buffer = new byte[1<<24];
        FASTOutput fastOutput = new FASTOutputByteArray(buffer );
        PrimitiveWriter writer = new PrimitiveWriter(writeBuffer, fastOutput, true);
               
        FASTRingBuffer ringBuffer = catalog.ringBuffers().buffers[0];
        FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(writer, ringBuffer, writerDispatch);
             
        //populate ring buffer with the new records to be written.
        
        float millionPerSecond = timeEncoding(fieldType, fieldCount, ringBuffer, dynamicWriter)/1000000f;        
        PrimitiveWriter.flush(writer);
        long bytesWritten = PrimitiveWriter.totalWritten(writer);
        assertEquals(0, PrimitiveWriter.bytesReadyToWrite(writer));
        
        //Use as bases for building single giant test file with test values provided, in ascii?
        totalWritten.addAndGet(PrimitiveWriter.totalWritten(writer));
        
        long nsLatency = FASTRingBufferConsumer.responseTime(ringBuffer.consumerData);
     
        //TODO: D, write to flat file to produce google chart.
        //System.err.println(TypeMask.xmlTypeName[fieldType]+" "+OperatorMask.xmlOperatorName[fieldOperator]+" fields: "+ fieldCount+" latency:"+nsLatency+"ns total mil per second "+millionPerSecond);
        //System.err.println("bytes written:"+bytesWritten);
        
//        //This visual check confirms that the write
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
        assertEquals(0xFF&Integer.parseInt("11000000", 2),0xFF&buffer[0]); //pmap to indicate that we do use template ID
        assertEquals(0xFF&Integer.parseInt("10000010", 2),0xFF&buffer[1]); //template id of 2

        
        FASTInput fastInput = new FASTInputByteArray(buffer, (int)bytesWritten);
        

        FASTInputReactor reactor = FAST.inputReactor(fastInput, catBytes);
      

        FASTRingBuffer[] buffers = reactor.ringBuffers();
        int buffersCount = buffers.length;
        
        int j = testRecordCount;
        while (j>0 && FASTInputReactor.pump(reactor)>=0) { //continue if there is no room or if a fragment is read.
        	int k = buffersCount;
        	while (j>0 && --k>=0) {
        		if (FASTRingBuffer.canMoveNext(buffers[k])) {
        			assertTrue(buffers[k].consumerData.isNewMessage());
        			assertEquals(testTemplateId, buffers[k].consumerData.messageId);
        			
        			//TODO: B, add test in here to confirm the values match
        			
        			j--;
        		}        		
        	}       	
        	
        }

    }


	private String byteString(int value) {
		String tmp = "00000000"+Integer.toBinaryString(value);
		return tmp.substring(tmp.length()-8, tmp.length());
	}

    //TODO: A, need the compiled static accessor to greatly simplify the usage of clients
    //TODO: A, need to review all misconfigured error messages to ensure that they are helpful and point in the right direction.
    

    private float timeEncoding(int fieldType, int fieldCount, FASTRingBuffer ringBuffer, FASTDynamicWriter dynamicWriter) {
       
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
                        FASTRingBufferWriter.writeInt(ringBuffer, testTemplateId);//template Id
                        int j = fieldCount;
                        while (--j>=0) {
                            FASTRingBufferWriter.writeInt(ringBuffer, ReaderWriterPrimitiveTest.unsignedIntData[--d]);
                            
                            if (0 == d) {
                                d = ReaderWriterPrimitiveTest.unsignedIntData.length;
                            }
                            
                        }
                        FASTRingBuffer.publishWrites(ringBuffer);
                        
                        if (FASTRingBuffer.canMoveNext(ringBuffer)) {//without move next we get no stats.
                            dynamicWriter.write();
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
                        FASTRingBufferWriter.writeInt(ringBuffer, testTemplateId);//template Id
                        int j = fieldCount;
                        while (--j>=0) {
                            FASTRingBufferWriter.writeLong(ringBuffer, ReaderWriterPrimitiveTest.unsignedLongData[--d]);
                            if (0==d) {
                                d = ReaderWriterPrimitiveTest.unsignedLongData.length;
                            }
                        }
                        FASTRingBuffer.publishWrites(ringBuffer);
                        if (FASTRingBuffer.canMoveNext(ringBuffer)) {//without move next we get no stats.
                            dynamicWriter.write();
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
                        FASTRingBufferWriter.writeInt(ringBuffer, testTemplateId);//template Id
                        int j = fieldCount;
                        while (--j>=0) {
                            FASTRingBufferWriter.writeDecimal(ringBuffer, exponent, ReaderWriterPrimitiveTest.unsignedLongData[--d]);
                            if (0==d) {
                                d = ReaderWriterPrimitiveTest.unsignedLongData.length;
                            }
                        }
                        FASTRingBuffer.publishWrites(ringBuffer);
                        if (FASTRingBuffer.canMoveNext(ringBuffer)) {//without move next we get no stats.
                            dynamicWriter.write();
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
                        FASTRingBufferWriter.writeInt(ringBuffer, testTemplateId);//template Id
                        int j = fieldCount;
                        while (--j>=0) {
                            //TODO: B, this test is not using UTF8 encoding for the UTF8 type mask!!!! this is only ASCII enoding always.
                            FASTRingBufferWriter.writeBytes(ringBuffer, ReaderWriterPrimitiveTest.stringDataBytes[--d]);
                            if (0==d) {
                                d = ReaderWriterPrimitiveTest.stringData.length;
                            }
                        }
                        FASTRingBuffer.publishWrites(ringBuffer);
                        if (FASTRingBuffer.canMoveNext(ringBuffer)) {//without move next we get no stats.
                            dynamicWriter.write();
                        }
                    }
                    long duration = System.nanoTime()-start;
                    return 1000000000f*testRecordCount/duration;
                    
                }
                
        
        }
        return 0;
    }



    private byte[] buildCatBytes(String name, int id, boolean reset, String dictionary, boolean fieldPresence,
            String fieldInitial, int fieldOperator, int fieldType, int f) {
        
        int fieldId = 1000;
        
        CatalogGenerator cg = new CatalogGenerator();
        TemplateGenerator template = cg.addTemplate(name, id, reset, dictionary);            
        while (--f>=0) {
            String fieldName = "field"+fieldId;
            template.addField(fieldName, fieldId++, fieldPresence, fieldType, fieldOperator, fieldInitial);        
        }
        
        StringBuilder builder = cg.appendTo("", new StringBuilder());        
   
        boolean debug = true;
        if (debug) {
        	System.err.println(builder);
        }
   
        
        ClientConfig clientConfig = new ClientConfig(21,19);  //keep bits small or the test will take a very long time to run.              
        byte[] catBytes = convertTemplateToCatBytes(builder, clientConfig);
        return catBytes;
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
    
            handler.postProcessing();
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
