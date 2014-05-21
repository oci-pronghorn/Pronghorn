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
import java.util.Arrays;
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

    final int PREAMBLE_IDX = 0;
    final int MESSAGE_ID_IDX = 1;
    final int VERSION_IDX = 2;
       
    @Test
    public void testClassReplacement() {        
        
        //These two are the same except for the internal version number
        byte[] catalog1=buildRawCatalogData("/performance/example.xml");
        byte[] catalog2=buildRawCatalogData("/performance/example2.xml");
                
        PrimitiveReader reader = buildReader("/performance/complex30000.dat");

        //setup
        int switchToCompiled1 = 50;
        int switchToCompiled2 = 100;
        int exitTest = 150;
        FASTClassLoader.deleteFiles();
        
        //Base class reference, known at static compile time.        
        FASTDecoder decoder;
        
        //create the first decoder (known at static compile time)
        decoder = new FASTReaderInterpreterDispatch(catalog1);
        System.err.println("Created new "+decoder.getClass().getSimpleName());
        
        int records=0;
        int flag;
        //Non-Blocking reactor select
        while (0!=(flag=FASTInputReactor.select(decoder, reader))) {
                 
            if ((0 != (flag & TemplateCatalog.END_OF_MESSAGE)))  {
                
               String version = FASTRingBufferReader.readText(decoder.ringBuffer(), 
                                                              VERSION_IDX, 
                                                              new StringBuilder()).toString();
               
               if (records<switchToCompiled1) {
                   //Interpreter
                   assertEquals("1.0",version);
               } else if (records<switchToCompiled2) {
                   //Compiled
                   assertEquals("1.0",version);
               } else if (records<exitTest) {
                   //Compiled 2
                   assertEquals("2.0",version);
               }               
               
               decoder.ringBuffer().dump(); //don't need the data but do need to empty the queue.
               
               records++;

               if (records==switchToCompiled1) {
                   decoder = DispatchLoader.loadDispatchReader(catalog1);
                   System.err.println("Created new "+decoder.getClass().getSimpleName());
               }
               if (records==switchToCompiled2) {
                   decoder = DispatchLoader.loadDispatchReader(catalog2);
                   System.err.println("Created new "+decoder.getClass().getSimpleName());
               }
               if (records>exitTest) {
                   break;
               }
           }
        }
  
    }


    private PrimitiveReader buildReader(String name) {
        URL sourceData = getClass().getResource(name);
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));      
        PrimitiveReader reader = new PrimitiveReader(buildBytesForTestingByteArray(sourceDataFile));
        return reader;
    }
    
    
    static byte[] buildRawCatalogData(String resourceName) {

        URL source = TemplateLoaderTest.class.getResource(resourceName);
        
        Properties properties = new Properties(); 
        properties.put(TemplateCatalog.KEY_PARAM_PREAMBLE_BYTES, "4");
        
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
