package com.ociweb.jfast.benchmark;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import com.google.caliper.Benchmark;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.loader.TemplateLoaderTest;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTInputReactor;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.RingBuffers;

public class Complex30000Benchmark extends Benchmark {

    FASTInputByteArray fastInput;
    PrimitiveReader reader;
    FASTReaderInterpreterDispatch readerDispatch;
    FASTInputReactor reactor;
    FASTRingBuffer queue;
    TemplateCatalogConfig catalog;
    byte[] testData;

    public Complex30000Benchmark() {
        catalog = new TemplateCatalogConfig(TemplateLoaderTest.buildRawCatalogData(new ClientConfig()));

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File fileSource = new File(sourceData.getFile());
        int maxPMapCountInBytes = TemplateCatalogConfig.maxPMapCountInBytes(catalog);   
        
        try {
            // do not want to time file access so copy file to memory
            testData = new byte[(int) fileSource.length()];
            FileInputStream inputStream = new FileInputStream(fileSource);
            int readBytes = inputStream.read(testData);
            inputStream.close();
            assertEquals(testData.length, readBytes);

            fastInput = new FASTInputByteArray(testData);
            reader = new PrimitiveReader(2048, fastInput, maxPMapCountInBytes);
            readerDispatch = new FASTReaderInterpreterDispatch(catalog);
            
            reactor = new FASTInputReactor(readerDispatch,reader);
            queue = RingBuffers.get(readerDispatch.ringBuffers,0);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void fastCore(FASTRingBuffer queue) {
        while (FASTInputReactor.pump(reactor)>=0) { //dump if no room to read or if we read a fragment
            FASTRingBuffer.dump(queue); // must dump values in buffer
        }
    }

    public void timeDecodeComplex30000(int reps) {

        while (--reps >= 0) {

            fastCore(queue);

            fastInput.reset();
            PrimitiveReader.reset(reader);
            FASTDecoder.reset(this.catalog.dictionaryFactory(), readerDispatch);

        }
    }


}
