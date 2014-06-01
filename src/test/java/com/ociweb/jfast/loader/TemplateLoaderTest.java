package com.ociweb.jfast.loader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.stream.DispatchObserver;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTInputReactor;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferReader;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;

public class TemplateLoaderTest {


    
    
    @Test
    public void buildRawCatalog() {

        byte[] catalogByteArray = buildRawCatalogData();
        assertEquals(697, catalogByteArray.length);
               
        
        // reconstruct Catalog object from stream
        TemplateCatalog catalog = new TemplateCatalog(catalogByteArray);

        boolean ok = false;
        int[] script = null;
        try {
            // /performance/example.xml contains 3 templates.
            assertEquals(3, catalog.templatesCount());

            script = catalog.fullScript();
            assertEquals(48, script.length);
            assertEquals(TypeMask.TextASCII, TokenBuilder.extractType(script[0]));// First
                                                                                  // Token

            // CMD:Group:010000/Close:PMap::010001/9
            assertEquals(TypeMask.Group, TokenBuilder.extractType(script[script.length - 1]));// Last
                                                                                              // Token

            ok = true;
        } finally {
            if (!ok) {
                System.err.println("Script Details:");
                if (null != script) {
                    System.err.println(convertScriptToString(script));
                }
            }
        }
    }

    private String convertScriptToString(int[] script) {
        StringBuilder builder = new StringBuilder();
        for (int token : script) {

            builder.append(TokenBuilder.tokenToString(token));

            builder.append("\n");
        }
        return builder.toString();
    }

    
    
    @Test
    public void testDecodeComplex30000() {
        
      Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        
        byte[] catBytes = buildRawCatalogData();
        TemplateCatalog catalog = new TemplateCatalog(catBytes); 

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
        long totalTestBytes = sourceDataFile.length();

        int maxPMapCountInBytes = 2 + ((Math.max(
                catalog.maxTemplatePMapSize(), catalog.maxNonTemplatePMapSize()) + 2) * catalog.getMaxGroupDepth());

      
        PrimitiveReader reader = new PrimitiveReader(buildBytesForTestingByteArray(sourceDataFile), maxPMapCountInBytes);
        
        FASTClassLoader.deleteFiles();
        
        FASTDecoder readerDispatch = DispatchLoader.loadDispatchReader(catBytes);
    //  readerDispatch = new FASTReaderInterpreterDispatch(catBytes);//not using compiled code
        System.err.println("using: "+readerDispatch.getClass().getSimpleName());
        System.gc();
        
        FASTRingBuffer queue = readerDispatch.ringBuffer(0);

        

        int warmup = 64;
        int count = 1024;
        int result = 0;
        int[] fullScript = catalog.scriptTokens;
        
        
        byte[] preamble = new byte[catalog.getIntProperty(TemplateCatalog.KEY_PARAM_PREAMBLE_BYTES,0)];

        int msgs = 0;
        int grps = 0;
        long queuedBytes = 0;
        int iter = warmup;
        while (--iter >= 0) {
            msgs = 0;
            grps = 0;
            int flag = 0; // same id needed for writer construction
            while (0 != (flag = FASTInputReactor.select(readerDispatch, reader, queue))) {
                // New flags
                // 0000 eof
                // 0001 has sequence group to read (may be combined with end of
                // message)
                // 0010 has message to read
                // neg unable to write to ring buffer

                // consumer code can stop only at end of message if desired or
                // for
                // lower latency can stop at the end of every sequence entry.
                // the call to hasMore is non-blocking with respect to the
                // consumer and
                // will return negative value if the ring buffer is full but it
                // will
                // spin lock if input stream is not ready.
                //

                if (0 != (flag & TemplateCatalog.END_OF_MESSAGE)) {
                    msgs++;

                    // this is a template message.
                    int bufferIdx = 0;
                    if (preamble.length > 0) {
                        int i = 0;
                        int s = preamble.length;
                        while (i < s) {
                            FASTRingBufferReader.readInt(queue, bufferIdx);
                            i += 4;
                            bufferIdx++;
                        }
                    }

                    int templateId = FASTRingBufferReader.readInt(queue, bufferIdx);
                    bufferIdx += 1;// point to first field
                    assertTrue("found " + templateId, 1 == templateId || 2 == templateId || 99 == templateId);

                    int i = catalog.templateStartIdx[templateId];
                    int limit = catalog.templateLimitIdx[templateId];
                    // System.err.println("new templateId "+templateId);
                    while (i < limit) {
                        int token = fullScript[i++];
                        // System.err.println("xxx:"+bufferIdx+" "+TokenBuilder.tokenToString(token));

                        if (isText(token)) {
                            queuedBytes += (4 * FASTRingBufferReader.readTextLength(queue, bufferIdx));
                        }

                        // find the next index after this token.
                        bufferIdx += stepSizeInRingBuffer(token);

                    }
                    queuedBytes += bufferIdx;// ring buffer bytes, NOT full
                                             // string byteVector data.

                    // must dump values in buffer or we will hang when reading.
                    // only dump at end of template not end of sequence.
                    // the removePosition must remain at the beginning until
                    // message is complete.
                    queue.dump();
                }
                grps++;

            }
            //fastInput.reset();
            PrimitiveReader.reset(reader);
            readerDispatch.reset();
            readerDispatch.reset(catalog.dictionaryFactory());
        }

        iter = count;
        while (--iter >= 0) {

            double start = System.nanoTime();

            int flag;
            while (0 != (flag = FASTInputReactor.select(readerDispatch, reader, queue))) {
                if (0 != (flag & TemplateCatalog.END_OF_MESSAGE)) {
                    result |= FASTRingBufferReader.readInt(queue, 0);
                    // must do some real work or
                    // hot-spot may delete this
                    // loop.
                                        
                 //TODO: A, how far to jump forward, can't know until the sequence size logic is resolved.                    
                 //   queue.removeForward(step);

                } else if (flag < 0) {
                    // negative flag indicates queue is backed up.
                    // must dump values in buffer or we will hang when reading.
                    FASTRingBufferReader.dump(queue);
                }
            }

            double duration = System.nanoTime() - start;
            if ((0x7F & iter) == 0) {
                int ns = (int) duration;
                float mmsgPerSec = (msgs * (float) 1000l / ns);
                float nsPerByte = (ns / (float) totalTestBytes);
                int mbps = (int) ((1000l * totalTestBytes * 8l) / ns);

                System.err.println("Duration:" + ns + "ns " + " " + mmsgPerSec + "MM/s " + " " + nsPerByte + "nspB "
                        + " " + mbps + "mbps " + " In:" + totalTestBytes + " Out:" + queuedBytes + " pct "
                        + (totalTestBytes / (float) queuedBytes) + " Messages:" + msgs + " Groups:" + grps); // Phrases/Clauses
                // Helps let us kill off the job.
            }

            // //////
            // reset the data to run the test again.
            // //////
            //fastInput.reset();
            PrimitiveReader.reset(reader);
            readerDispatch.reset();
            readerDispatch.reset(catalog.dictionaryFactory());

        }
        assertTrue(result != 0);

    }

    private boolean isText(int token) {
        return 0x08 == (0x1F & (token >>> TokenBuilder.SHIFT_TYPE));
    }

    private int stepSizeInRingBuffer(int token) {
        int stepSize = 0;
        if (0 == (token & (16 << TokenBuilder.SHIFT_TYPE))) {
            // 0????
            if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                // 00???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    // int
                    stepSize = 1;
                } else {
                    // long
                    stepSize = 2;
                }
            } else {
                // 01???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    // int for text (takes up 2 slots)
                    stepSize = 2;
                } else {
                    // 011??
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        // 0110? Decimal and DecimalOptional
                        stepSize = 3;
                    } else {
                        // int for bytes
                        stepSize = 0;// BYTES ARE NOT IMPLEMENTED YET BUT WILL
                                     // BE 2;
                    }
                }
            }
        } else {
            if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                // 10???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    // 100??
                    // Group Type, no others defined so no need to keep checking
                    stepSize = 0;
                } else {
                    // 101??
                    // Length Type, no others defined so no need to keep
                    // checking
                    // Only happens once before a node sequence so push it on
                    // the count stack
                    stepSize = 1;
                }
            } else {
                // 11???
                // Dictionary Type, no others defined so no need to keep
                // checking
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
            MappedByteBuffer mem = fc.map(FileChannel.MapMode.READ_ONLY, 0, totalTestBytes);
            fastInput = new FASTInputByteBuffer(mem);
            fc.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fastInput;
    }



    @Test
    public void testDecodeEncodeComplex30000() {
        byte[] catBytes = buildRawCatalogData();
        final TemplateCatalog catalog = new TemplateCatalog(catBytes);

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
        long totalTestBytes = sourceDataFile.length();

        FASTInputByteArray fastInput = buildInputForTestingByteArray(sourceDataFile);

        // New memory mapped solution. No need to cache because we warm up and
        // OS already has it.
        // FASTInputByteBuffer fastInput =
        // buildInputForTestingByteBuffer(sourceDataFile);

        PrimitiveReader reader = new PrimitiveReader(2048, fastInput, 32);
        
        FASTDecoder readerDispatch = DispatchLoader.loadDispatchReader(catBytes);
       // readerDispatch = new FASTReaderInterpreterDispatch(catBytes);//not using compiled code
        
        FASTRingBuffer queue = readerDispatch.ringBuffer(0);

        byte[] targetBuffer = new byte[(int) (totalTestBytes)];
        FASTOutputByteArray fastOutput = new FASTOutputByteArray(targetBuffer);

        // TODO: Z, force this error and add friendly message, when minimize
        // latency set to false these need to be much bigger?
        int writeBuffer = 2048;
        int maxGroupCount = 3;// NOTE: may need to be VERY large if minimize
                              // latency is turned off!!
        PrimitiveWriter writer = new PrimitiveWriter(writeBuffer, fastOutput, maxGroupCount, true);
        FASTWriterInterpreterDispatch writerDispatch = new FASTWriterInterpreterDispatch(catalog.dictionaryFactory(), catalog.templatesCount(),
                queue, catalog.maxNonTemplatePMapSize(), catalog.dictionaryResetMembers(),
                catalog.fullScript(), catalog.getMaxGroupDepth());

        FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(writer, catalog, queue, writerDispatch);

        final Map<Long, String> reads = new HashMap<Long, String>();
        readerDispatch.setDispatchObserver(new DispatchObserver() {

            @Override
            public void tokenItem(long absPos, int token, int cursor, String value) {
                String msg = "\n    R_" + TokenBuilder.tokenToString(token) + " id:"
                        + (cursor >= catalog.scriptFieldIds.length ? "ERR" : "" + catalog.scriptFieldIds[cursor])
                        + " curs:" + cursor + " tok:" + token + " " + value;
                if (reads.containsKey(absPos)) {
                    msg = reads.get(absPos) + " " + msg;
                }
                reads.put(absPos, msg);
            }
        });

        System.gc();

        int warmup = 20;// set much larger for profiler
        int count = 128;

        long wroteSize = 0;
        int msgs = 0;
        int grps = 0;
        int iter = warmup;
        while (--iter >= 0) {
            msgs = 0;
            grps = 0;
            int flags = 0; // same id needed for writer construction
            while (0 != (flags = FASTInputReactor.select(readerDispatch, reader, queue))) {
                while (queue.hasContent()) {
                    dynamicWriter.write();
                }

                if (0 != (flags & TemplateCatalog.END_OF_MESSAGE)) {
                    msgs++;
                }
                grps++;
            }

            queue.reset();

            fastInput.reset();
            PrimitiveReader.reset(reader);
            readerDispatch.reset();
            readerDispatch.reset(catalog.dictionaryFactory());

            writer.flush(writer);
            wroteSize = Math.max(wroteSize, writer.totalWritten(writer));
            fastOutput.reset();
            writer.reset(writer);
            dynamicWriter.reset(true);

            // only need to collect data on the first run
            readerDispatch.setDispatchObserver(null);
            writerDispatch.setDispatchObserver(null);
        }

        scanForMismatch(targetBuffer, fastInput.getSource(), reads);
        // Expected total read fields:2126101
        assertEquals("test file bytes", totalTestBytes, wroteSize);

        iter = count;
        while (--iter >= 0) {

            double start = System.nanoTime();
            while (0 != FASTInputReactor.select(readerDispatch, reader, queue)) {
                while (queue.hasContent()) {
                    dynamicWriter.write();
                }
            }
            double duration = System.nanoTime() - start;

            if ((0x3F & iter) == 0) {
                int ns = (int) duration;
                float mmsgPerSec = (msgs * (float) 1000l / ns);
                float nsPerByte = (ns / (float) totalTestBytes);
                int mbps = (int) ((1000l * totalTestBytes * 8l) / ns);

                System.err.println("Duration:" + ns + "ns " + " " + mmsgPerSec + "MM/s " + " " + nsPerByte + "nspB "
                        + " " + mbps + "mbps " + " Bytes:" + totalTestBytes + " Messages:" + msgs + " Groups:" + grps); // Phrases/Clauses
            }

            // //////
            // reset the data to run the test again.
            // //////
            queue.reset();

            fastInput.reset();
            PrimitiveReader.reset(reader);
            readerDispatch.reset();
            readerDispatch.reset(catalog.dictionaryFactory());

            fastOutput.reset();
            writer.reset(writer);
            dynamicWriter.reset(true);

        }

    }

    private void scanForMismatch(byte[] targetBuffer, byte[] sourceBuffer, Map<Long, String> reads) {
        int lookAhead = 11;
        int maxDisplay = 31;
        int nthErr = 1;

        int i = 0;
        int err = 0;
        int displayed = 0;
        while (i < sourceBuffer.length && displayed < maxDisplay) {
            // Check data for mismatch
            if (i + lookAhead < sourceBuffer.length && i + lookAhead < targetBuffer.length
                    && sourceBuffer[i + lookAhead] != targetBuffer[i + lookAhead]) {
                err++;
                ;
            }

            if (err >= nthErr) {
                displayed++;
                StringBuilder builder = new StringBuilder();
                builder.append(i).append(' ').append(" R").append(hex(sourceBuffer[i])).append(' ').append(" W")
                        .append(hex(targetBuffer[i])).append(' ');

                builder.append(" R").append(bin(sourceBuffer[i])).append(' ');
                builder.append(" W").append(bin(targetBuffer[i])).append(' ');

                if (sourceBuffer[i] != targetBuffer[i]) {
                    builder.append(" ***ERROR***  decimals " + (0x7F & sourceBuffer[i]) + "  "
                            + (0x7F & targetBuffer[i]) + " ascii "
                            + Character.toString((char) (0x7F & sourceBuffer[i])) + "  "
                            + Character.toString((char) (0x7F & targetBuffer[i])));
                }

                Long lng = Long.valueOf(i);
                if (reads.containsKey(lng)) {
                    builder.append(reads.get(lng)).append(' ');
                } else {
                    builder.append("                ");
                }

                System.err.println(builder);

            }
            i++;
        }
    }

    private String hex(int x) {
        String t = Integer.toHexString(0xFF & x);
        if (t.length() == 1) {
            return '0' + t;
        } else {
            return t;
        }
    }

    private String bin(int x) {
        String t = Integer.toBinaryString(0xFF & x);
        while (t.length() < 8) {
            t = '0' + t;
        }

        return t.substring(t.length() - 8);

    }

    static FASTInputByteArray buildInputForTestingByteArray(File fileSource) {
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

        FASTInputByteArray fastInput = new FASTInputByteArray(fileData);
        return fastInput;
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

//    private String hexString(byte[] targetBuffer) {
//        StringBuilder builder = new StringBuilder();
//
//        for (byte b : targetBuffer) {
//
//            String tmp = Integer.toHexString(0xFF & b);
//            builder.append(tmp.substring(Math.max(0, tmp.length() - 2))).append(" ");
//
//        }
//        return builder.toString();
//    }

//    private String binString(byte[] targetBuffer) {
//        StringBuilder builder = new StringBuilder();
//
//        for (byte b : targetBuffer) {
//
//            String tmp = Integer.toBinaryString(0xFF & b);
//            builder.append(tmp.substring(Math.max(0, tmp.length() - 8))).append(" ");
//
//        }
//        return builder.toString();
//    }

    public static byte[] buildRawCatalogData() {
        //this example uses the preamble feature
        Properties properties = new Properties(); 
        properties.put(TemplateCatalog.KEY_PARAM_PREAMBLE_BYTES, "4");

        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            TemplateLoader.buildCatalog(catalogBuffer, "/performance/example.xml", properties);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertTrue("Catalog must be built.", catalogBuffer.size() > 0);

        byte[] catalogByteArray = catalogBuffer.toByteArray();
        return catalogByteArray;
    }


}
