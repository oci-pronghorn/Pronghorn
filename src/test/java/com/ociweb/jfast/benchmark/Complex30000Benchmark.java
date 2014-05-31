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
import java.util.Properties;

import org.openfast.Message;
import org.openfast.MessageInputStream;
import org.openfast.impl.CmeMessageBlockReader;
import org.openfast.template.MessageTemplate;
import org.openfast.template.loader.MessageTemplateLoader;
import org.openfast.template.loader.XMLMessageTemplateLoader;

import com.google.caliper.Benchmark;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.loader.TemplateLoader;
import com.ociweb.jfast.loader.TemplateLoaderTest;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTInputReactor;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferReader;

public class Complex30000Benchmark extends Benchmark {

    FASTInputByteArray fastInput;
    PrimitiveReader reader;
    FASTReaderInterpreterDispatch readerDispatch;
    FASTRingBuffer queue;
    TemplateCatalog catalog;
    byte[] testData;

    public Complex30000Benchmark() {
        catalog = new TemplateCatalog(TemplateLoaderTest.buildRawCatalogData());

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File fileSource = new File(sourceData.getFile());

        try {
            // do not want to time file access so copy file to memory
            testData = new byte[(int) fileSource.length()];
            FileInputStream inputStream = new FileInputStream(fileSource);
            int readBytes = inputStream.read(testData);
            inputStream.close();
            assertEquals(testData.length, readBytes);

            fastInput = new FASTInputByteArray(testData);
            reader = new PrimitiveReader(2048, fastInput, 32);
            readerDispatch = new FASTReaderInterpreterDispatch(catalog);
            queue = readerDispatch.ringBuffer(0);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private int fastCore(int result, FASTRingBuffer queue) {
        int flag;
        while (0 != (flag = FASTInputReactor.select(readerDispatch, reader, queue))) {
            if (0 != (flag & 0x02)) {
                result |= FASTRingBufferReader.readInt(queue, 0);// must do some
                                                                 // real work or
                                                                 // hot-spot may
                                                                 // delete this
                                                                 // loop.
                queue.dump(); // must dump values in buffer or we will hang when
                              // reading.
            }
        }
        return result;
    }

    public int timeDecodeComplex30000(int reps) {

        int result = 0;
        while (--reps >= 0) {

            fastCore(result, queue);

            fastInput.reset();
            PrimitiveReader.reset(reader);
            readerDispatch.reset();

        }
        return result;
    }

    // public int timeDecodeComplex30000ResetOverhead(int reps) {
    //
    // FASTRingBuffer queue = dynamicReader.ringBuffer();
    //
    // int result = 0;
    // while (--reps>=0) {
    //
    //
    // fastInput.reset();
    // primitiveReader.reset();
    // dynamicReader.reset();
    //
    // }
    // return result;
    // }

    public void openFastTest() {
        try {
            MessageTemplateLoader loader = new XMLMessageTemplateLoader();
            URL source = getClass().getResource("/performance/example.xml");
            InputStream aStream = new FileInputStream(new File(source.getFile()));
            MessageTemplate[] templates = loader.load(aStream);
            // System.err.println("templates count "+templates.length);
            // System.err.println(templates[0].getId());

            source = getClass().getResource("/performance/complex30000.dat");
            File fileSource = new File(source.getFile());
            // do not want to time file access so copy file to memory
            byte[] fileData = new byte[(int) fileSource.length()];
            FileInputStream inputStream = new FileInputStream(fileSource);
            int readBytes = inputStream.read(fileData);
            inputStream.close();
            assertEquals(fileData.length, readBytes);

            // /
            // /
            InputStream fastEncodedStream = new ByteArrayInputStream(fileData);

            MessageInputStream messageIn = new MessageInputStream(fastEncodedStream);

            // must add support for the 4 byte preamble
            messageIn.setBlockReader(new CmeMessageBlockReader());

            messageIn.setTemplateRegistry(loader.getTemplateRegistry());

            Message msg;
            while (null != (msg = messageIn.readMessage())) {
                // System.err.println(msg.getFieldCount());
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
