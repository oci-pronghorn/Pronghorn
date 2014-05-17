package com.ociweb.jfast.generator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.loader.TemplateLoader;
import com.ociweb.jfast.loader.TemplateLoaderTest;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTInputReactor;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferReader;

public class DispatchLoaderTest {


    //On IBM JVM this may hold the classes in permGen space longer than expected, but they are GC.
    //by keeping a ref that class and its loader must stay arround. and we will hit perm gen error.
    
    @Test
    public void test() {        
        
        //These two are the same except for the internal version number
        byte[] catalog1=buildRawCatalogData("/performance/example.xml");
        byte[] catalog2=buildRawCatalogData("/performance/example2.xml");
        
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));      
        PrimitiveReader reader = new PrimitiveReader(buildBytesForTestingByteArray(sourceDataFile),40);

        //Base class reference, known at static compile time.        
        FASTDecoder decoder;
        
                
        decoder = new FASTReaderInterpreterDispatch(catalog1);
        FASTRingBuffer ringBuffer = decoder.ringBuffer(); //Data comes from here.
        
        int messageIdIdx = 1;//0 is the preamble
        
        int triggerRecord1 = 50;
        int triggerRecord2 = 99;
        
        int records=0;
        //Non-Blocking reactor dispatch
        int flag;
        while (0!=(flag=FASTInputReactor.select(decoder, reader))) {
                 
           // boolean x = (0 == (flag & TemplateCatalog.END_OF_SEQ_ENTRY));
            boolean y = (0 != (flag & TemplateCatalog.END_OF_MESSAGE));
            
           if (y)  {
               
               int messageId = FASTRingBufferReader.readInt(ringBuffer, messageIdIdx);
               String version = FASTRingBufferReader.readText(ringBuffer, messageIdIdx+1, new StringBuilder()).toString();
               System.err.println(messageId+" "+version+" flag:"+flag);
               
               
               ringBuffer.dump(); //don't need the data but do need to empty the queue.
               
               records++;

//               if (records==triggerRecord1) {
//                   //TODO: this load lost the dictonary!
//                   decoder = DispatchLoader.loadDispatchReader(catalog1);
//                   ringBuffer = decoder.ringBuffer();
//               }
//               if (records==triggerRecord2) {
//                   decoder = DispatchLoader.loadDispatchReader(catalog2);
//                   ringBuffer = decoder.ringBuffer();
//               }
           }
        }
        
        //TODO: must add nested groups in ring buffer and fetch ID.
        
        
        
    }
    
    
    static byte[] buildRawCatalogData(String resourceName) {

        URL source = TemplateLoaderTest.class.getResource(resourceName);
        
        Properties properties = new Properties(); 
        properties.put(TemplateCatalog.KEY_PARAM_PREAMBLE_BYTES, "4");//TODO: when this is missing we get exception in odd places, what can be done to help make those error esier to find?
        
        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            File file = new File(source.toURI());
            assertTrue(file.exists());
            TemplateLoader.buildCatalog(catalogBuffer, file, properties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return catalogBuffer.toByteArray();
    }

    static byte[] buildBytesForTestingByteArray(File fileSource) {
        byte[] fileData = null;
        try {
            // do not want to time file access so copy file to memory
            fileData = new byte[(int) fileSource.length()];
            FileInputStream inputStream = new FileInputStream(fileSource);
            int readBytes = inputStream.read(fileData);
            inputStream.close();
            assertEquals(fileData.length, readBytes);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileData;
    }
}
