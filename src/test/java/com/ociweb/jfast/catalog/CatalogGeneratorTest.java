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
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

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
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferConsumer;
import com.ociweb.jfast.stream.FASTRingBufferWriter;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;
import com.ociweb.jfast.util.Stats;

//TODO B, the example test file is full of long sequences of 1 then a 2,  if the ratio was more balanced this file could be read in parallel with multiple decoders.
//TODO B, note that template 1 (the most common) also has a reset on each message, As a result each of these can be done in parallel.
//TODO B, build speed loader of file with NIO and add multiple decoders

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
            //OperatorMask.Field_Copy,
           // OperatorMask.Field_Default,
           //  OperatorMask.Field_Increment,            
            OperatorMask.Field_Delta,
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
    
   
    private final int writeBuffer=65536;
    
    //If this test is going to both encode then decode to test both parts of the process this
    //buffer must be very large in order to hold all the possible permutations
    private final byte[] buffer = new byte[1<<28];
    
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
        int id = 2;
        boolean reset = false;
        String dictionary = null;
        boolean fieldPresence = false;     
        String fieldInitial = "10";        
        int totalFields = 400;//1024;  //at 4 bytes per int for 4K message size test                              
        
        int p = numericOps.length;
        while (--p>=0) {            
            int fieldOperator = numericOps[p];            
            int t = numericTypes.length;
            while (--t>=0) {               
                int fieldType = numericTypes[t];
                int fieldCount = 1; 
                while (fieldCount<totalFields) {                 
                    byte[] catBytes = buildCatBytes(name, id, reset, dictionary, fieldPresence, fieldInitial, fieldOperator, fieldType, fieldCount);  
                    
                    
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
        
        int i = numericCatalogs.size();
        System.out.println("testing "+i+" numeric configurations");
        while (--i>=0) {
            testEncoding(numericFieldOperators.get(i).intValue(), 
                         numericFieldTypes.get(i).intValue(), 
                         numericFieldCounts.get(i).intValue(), 
                         numericCatalogs.get(i));
        }
            
    }


    int lastOp = -1;
    int lastType = -1;
    int lastFieldCount = -1;

    public void testEncoding(int fieldOperator, int fieldType, int fieldCount, byte[] catBytes) {
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
            
   //         boolean noPMap = (0 == (catalog.getScriptTokens()[0] & (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER)));
  //          System.err.println("noPMap:"+noPMap);
            
//            for (int token:  catalog.getScriptTokens()) {
//                System.err.println("    "+TokenBuilder.tokenToString(token));
//            }
            
        }
//        if (fieldCount!=lastFieldCount) {
//            lastFieldCount = fieldCount;
//            System.err.println("FieldCount:"+lastFieldCount);
//            
//        }
        
        
        
        
        assertEquals(1, catalog.templatesCount());
        int maxGroupCount=4096;
        
        //TODO: A, new unit tests. use catalog to test mock data
        FASTClassLoader.deleteFiles();
        FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriter(catBytes); 
                        
   
        FASTOutput fastOutput = new FASTOutputByteArray(buffer );
        //TODO: A, need the maximum groups that would fit in this buffer based on smallest known buffer.
        //catalog.getMaxGroupDepth()
        
        PrimitiveWriter writer = new PrimitiveWriter(writeBuffer, fastOutput, maxGroupCount, false);
                        
            //    catalog.dictionaryFactory().byteDictionary().
               
        FASTRingBuffer queue = catalog.ringBuffers().buffers[0];
        FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(writer, queue, writerDispatch);
             
        //populate ring buffer with the new records to be written.
        
        float millionPerSecond = timeEncoding(fieldType, fieldCount, queue, dynamicWriter)/1000000f;
        
        long nsLatency = FASTRingBufferConsumer.responseTime(queue.consumerData);
        
        System.err.println(TypeMask.xmlTypeName[fieldType]+" "+OperatorMask.xmlOperatorName[fieldOperator]+" fields: "+ fieldCount+" latency:"+nsLatency+"ns total mil per second "+millionPerSecond);
                //" per field "+(responseTime/(double)fieldCount));
        
        
        //followed by time decoding
        //followed by validate values
        
        //TypeMask.xmlTypeName
        
        
        //TODO A, need the compiled static accessor to greatly simplify the usage of clients
        //TODO A, need to review all misconfigured error messages to ensure that they are helpful and point in the right direction.
               
        
//        int j = fieldCount;
//        while (--j>=0) {
//            FASTRingBufferWriter.writeInt(queue, 42);
//        }
//        FASTRingBuffer.unBlockFragment(queue.headPos,queue.workingHeadPos);
//        
//        
//        
//        dynamicWriter.write();
        
        
        
    }


    private float timeEncoding(int fieldType, int fieldCount, FASTRingBuffer queue, FASTDynamicWriter dynamicWriter) {

        int size = queue.maxSize;
        
        int records = 100000; //testing enough to get repeatable results
        int d;
        switch(fieldType) {
            case TypeMask.IntegerUnsigned:
            case TypeMask.IntegerUnsignedOptional:
            case TypeMask.IntegerSigned:
            case TypeMask.IntegerSignedOptional:
                {
     //               records = size/((fieldCount)+1);   
                    long start = System.nanoTime();
                    
                    d = ReaderWriterPrimitiveTest.unsignedIntData.length;
                    int i = records;
                    while (--i>=0) {
                        FASTRingBufferWriter.writeInt(queue, 0);//template Id
                        int j = fieldCount;
                        while (--j>=0) {
                            FASTRingBufferWriter.writeInt(queue, ReaderWriterPrimitiveTest.unsignedIntData[--d]);
                            if (0==d) {
                                d = ReaderWriterPrimitiveTest.unsignedIntData.length;
                            }
                        }
                        FASTRingBuffer.unBlockFragment(queue.headPos,queue.workingHeadPos);
                        
                        if (FASTRingBuffer.moveNext(queue)) {//without move next we get no stats.
                            dynamicWriter.write();
                        }
                    }
                    long duration = System.nanoTime()-start;
                    return 1000000000f*records/duration;
                }
            case TypeMask.LongUnsigned:
            case TypeMask.LongUnsignedOptional:
            case TypeMask.LongSigned:
            case TypeMask.LongSignedOptional:
                {
                    records = size/((2*fieldCount)+1);   
                    long start = System.nanoTime();
                    
                    d = ReaderWriterPrimitiveTest.unsignedLongData.length;
                    int i = records;
                    while (--i>=0) {
                        FASTRingBufferWriter.writeInt(queue, 0);//template Id
                        int j = fieldCount;
                        while (--j>=0) {
                            FASTRingBufferWriter.writeLong(queue, ReaderWriterPrimitiveTest.unsignedLongData[--d]);
                            if (0==d) {
                                d = ReaderWriterPrimitiveTest.unsignedLongData.length;
                            }
                        }
                        FASTRingBuffer.unBlockFragment(queue.headPos,queue.workingHeadPos);
                        if (FASTRingBuffer.moveNext(queue)) {//without move next we get no stats.
                            dynamicWriter.write();
                        }
                    }
                    long duration = System.nanoTime()-start;
                    return 1000000000f*records/duration;
                }
            case TypeMask.Decimal:
            case TypeMask.DecimalOptional:
                //ReaderWriterPrimitiveTest.unsignedLongData;
                {
                    records = size/((3*fieldCount)+1);
                    long start = System.nanoTime();
                    
                    int exponent = 2;
                    d = ReaderWriterPrimitiveTest.unsignedLongData.length;
                    int i = records;
                    while (--i>=0) {
                        FASTRingBufferWriter.writeInt(queue, 0);//template Id
                        int j = fieldCount;
                        while (--j>=0) {
                            FASTRingBufferWriter.writeDecimal(queue, exponent, ReaderWriterPrimitiveTest.unsignedLongData[--d]);
                            if (0==d) {
                                d = ReaderWriterPrimitiveTest.unsignedLongData.length;
                            }
                        }
                        FASTRingBuffer.unBlockFragment(queue.headPos,queue.workingHeadPos);
                        if (FASTRingBuffer.moveNext(queue)) {//without move next we get no stats.
                            dynamicWriter.write();
                        }
                    }
                    long duration = System.nanoTime()-start;
                    return 1000000000f*records/duration;
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
   
      //         System.err.println(builder);
        
        ClientConfig clientConfig = new ClientConfig(13,10);  //keep bits small or the test will take a very long time to run.              
        byte[] catBytes = convertTemplateToCatBytes(builder, clientConfig);
        return catBytes;
    }



    public byte[] convertTemplateToCatBytes(StringBuilder builder, ClientConfig clientConfig) {
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
