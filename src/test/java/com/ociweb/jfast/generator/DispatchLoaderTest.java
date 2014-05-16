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

import com.ociweb.jfast.loader.TemplateLoader;
import com.ociweb.jfast.loader.TemplateLoaderTest;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTRingBuffer;

public class DispatchLoaderTest {

    //TODO: AA * Finish Russ site
    //TODO: AA * Finish unit tests here and refactoring.
    
    //On IBM JVM this may hold the classes in permGen space longer than expected, but they are GC.
    //by keeping a ref that class and its loader must stay arround. and we will hit perm gen error.
    
    @Test
    public void test() {
        
        
        //These two are the same except for the internal version number
        byte[] catalog1=buildRawCatalogData("/performance/example.xml");
        byte[] catalog2=buildRawCatalogData("/performance/example2.xml");
        
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));      
        PrimitiveReader reader = new PrimitiveReader(buildBytesForTestingByteArray(sourceDataFile));

        //Base class reference, known at static compile time.        
        FASTDecoder decoder;
        
    //    FASTRingBuffer target = new FASTRingBuffer();
        
        decoder = DispatchLoader.loadDispatchReader(catalog1); //pass in target
        
        
        
        decoder.decode(reader);//pump
        
        //Ring bufer is bound to catalog becuase it can inject the constants by reference, eg no copy.
        FASTRingBuffer buffer = decoder.ringBuffer();
        //The message type offsets are also bound to catalog so they should be in buffer.
        
        
        //read from target
        //read message id from target.
        
        
        decoder = DispatchLoader.loadDispatchReader(catalog2);
        decoder.decode(reader);
        

        //demo memory leak?
//        List<FASTDecoder> x = new ArrayList<FASTDecoder>();
//        while(true) {
//        decoder = DispatchLoader.loadDispatchReader(catalog1);
//         x.add(decoder);
////       // decoder.decode(reader)
//        
//        decoder = DispatchLoader.loadDispatchReader(catalog2);
//        x.add(decoder);
//        }
//        //decoder.decode(reader)
        
    }
    
    
    static byte[] buildRawCatalogData(String resourceName) {

        URL source = TemplateLoaderTest.class.getResource(resourceName);
        
        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            File file = new File(source.toURI());
            assertTrue(file.exists());
            Properties properties = new Properties(); //TODO: load from file or args?
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
