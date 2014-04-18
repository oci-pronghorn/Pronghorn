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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.stream.DispatchObserver;
import com.ociweb.jfast.stream.FASTDynamicReader;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTReaderDispatch;
import com.ociweb.jfast.stream.FASTReaderDispatchGenExample;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTWriterDispatch;

public class TemplateLoaderTest {

    @Test
    public void buildRawCatalog() {

        byte[] catalogByteArray = buildRawCatalogData();

        // reconstruct Catalog object from stream
        FASTInput input = new FASTInputByteArray(catalogByteArray);
        TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(input));

        boolean ok = false;
        int[] script = null;
        try {
            // /performance/example.xml contains 3 templates.
            assertEquals(3, catalog.templatesCount());
            assertEquals(431, catalogByteArray.length);

            script = catalog.fullScript();
            assertEquals(46, script.length);
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

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");

        FASTInputByteArray fastInput = buildInputForTestingByteArray(new File(sourceData.getFile()));
        int totalTestBytes = fastInput.remaining();
        int bufferSize = 4096;
        PrimitiveReader primitiveReader = new PrimitiveReader(bufferSize, fastInput, (2 + ((Math.max(
                catalog.maxTemplatePMapSize(), catalog.maxNonTemplatePMapSize()) + 2) * catalog.getMaxGroupDepth())));
        FASTReaderDispatch readerDispatch = new FASTReaderDispatch(primitiveReader, catalog.dictionaryFactory(), 3,
                catalog.dictionaryMembers(), catalog.getMaxTextLength(), catalog.getMaxByteVectorLength(),
                catalog.getTextGap(), catalog.getByteVectorGap(), catalog.fullScript(), catalog.getMaxGroupDepth(), 8,
                7);
        FASTDynamicReader dynamicReader = new FASTDynamicReader(primitiveReader, catalog, readerDispatch);
        FASTRingBuffer queue = readerDispatch.ringBuffer();

        System.gc();

        int count = 5;
        int result = 0;

        int iter = count;
        while (--iter >= 0) {

            double start = System.nanoTime();

            int flag;
            while (0 != (flag = dynamicReader.hasMore())) {
                if (0 != (flag & TemplateCatalog.END_OF_MESSAGE)) {
                    result |= queue.readInteger(0);// must do some real work or
                                                   // hot-spot may delete this
                                                   // loop.
                    queue.dump(); // must dump values in buffer or we will hang
                                  // when reading.
                }
            }

            double duration = System.nanoTime() - start;

            int ns = (int) (duration / count);
            float nsPerByte = (ns / (float) totalTestBytes);
            int mbps = (int) ((1000l * totalTestBytes * 8l) / ns);

            System.err.println("Duration:" + ns + "ns " + " " + nsPerByte + "nspB " + " " + mbps + "mbps " + " Bytes:"
                    + totalTestBytes); // Phrases/Clauses

            // //////
            // reset the data to run the test again.
            // //////
            fastInput.reset();
            primitiveReader.reset();
            dynamicReader.reset(true);

        }

    }

    @Test
    public void testDecodeComplex30000() {

       // new SourceTemplates();
        
        FASTInput templateCatalogInput = new FASTInputByteArray(buildRawCatalogData());
        TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(templateCatalogInput));

        // values which need to be set client side and are not in the template.
        catalog.setMessagePreambleSize((byte) 4);
        catalog.setMaxByteVectorLength(0, 0);// byte vectors are unused
        catalog.setMaxTextLength(14, 8);

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
        long totalTestBytes = sourceDataFile.length();

        // this is much faster because we no longer need to jump out to OS.
        FASTInputByteArray fastInput = buildInputForTestingByteArray(sourceDataFile);

        // New memory mapped solution. No need to cache because we warm up and
        // OS already has it.
        // FASTInputByteBuffer fastInput =
        // buildInputForTestingByteBuffer(sourceDataFile);

        /*
         * nate@Noah:~/github$ sudo vim /etc/sysctl.conf nate@Noah:~/github$
         * sysctl -p sysctl: permission denied on key 'vm.zone_reclaim_mode'
         * nate@Noah:~/github$ sudo sysctl -p vm.zone_reclaim_mode = 0
         * nate@Noah:~/github$
         */
        int bufferSize = 4096;// do not change without testing, 4096 is ideal.
        PrimitiveReader primitiveReader = new PrimitiveReader(bufferSize, fastInput, (2 + ((Math.max(
                catalog.maxTemplatePMapSize(), catalog.maxNonTemplatePMapSize()) + 2) * catalog.getMaxGroupDepth())));
        FASTReaderDispatch readerDispatch = new FASTReaderDispatch(          // GenExample(
                primitiveReader, catalog.dictionaryFactory(), 3, catalog.dictionaryMembers(),
                catalog.getMaxTextLength(), catalog.getMaxByteVectorLength(), catalog.getTextGap(),
                catalog.getByteVectorGap(), catalog.fullScript(), catalog.getMaxGroupDepth(), 8, 7);
        FASTDynamicReader dynamicReader = new FASTDynamicReader(primitiveReader, catalog, readerDispatch);
        FASTRingBuffer queue = readerDispatch.ringBuffer();

        // TODO: X, look into core affinity

        // TODO: B, Use generated class if found else use slower base class
        // behavior.
        // TODO: B, generator code should take TemplateCatalog to build class if
        // needed.

        int warmup = 20;
        int count = 1024;
        int result = 0;
        int[] fullScript = catalog.scriptTokens;
        byte[] preamble = new byte[catalog.preambleSize];

        int msgs = 0;
        int grps = 0;
        long queuedBytes = 0;
        int iter = warmup;
        while (--iter >= 0) {
            msgs = 0;
            grps = 0;
            int flag = 0; // same id needed for writer construction
            while (0 != (flag = dynamicReader.hasMore())) {
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
                            queue.readInteger(bufferIdx);
                            i += 4;
                            bufferIdx++;
                        }
                    }

                    int templateId = queue.readInteger(bufferIdx);
                    bufferIdx += 1;// point to first field
                    assertTrue("found " + templateId, 1 == templateId || 2 == templateId || 99 == templateId);

                    int i = catalog.getTemplateStartIdx(templateId);
                    int limit = catalog.getTemplateLimitIdx(templateId);
                    // System.err.println("new templateId "+templateId);
                    while (i < limit) {
                        int token = fullScript[i++];
                        // System.err.println("xxx:"+bufferIdx+" "+TokenBuilder.tokenToString(token));

                        if (isText(token)) {
                            queuedBytes += (4 * queue.readCharsLength(bufferIdx));
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
            fastInput.reset();
            primitiveReader.reset();
            dynamicReader.reset(true);
        }

        iter = count;
        while (--iter >= 0) {

            double start = System.nanoTime();

            int flag;
            while (0 != (flag = dynamicReader.hasMore())) {
                if (0 != (flag & TemplateCatalog.END_OF_MESSAGE)) {
                    result |= queue.readInteger(0);// must do some real work or
                                                   // hot-spot may delete this
                                                   // loop.
                } else if (flag < 0) {// negative flag indicates queue is backed
                                      // up.
                    queue.dump(); // must dump values in buffer or we will hang
                                  // when reading.
                }
            }

            double duration = System.nanoTime() - start;
            Thread.yield();
            if ((0x7F & iter) == 0) {
                int ns = (int) duration;
                float mmsgPerSec = (msgs * (float) 1000l / ns);
                float nsPerByte = (ns / (float) totalTestBytes);
                int mbps = (int) ((1000l * totalTestBytes * 8l) / ns);

                System.err.println("Duration:" + ns + "ns " + " " + mmsgPerSec + "MM/s " + " " + nsPerByte + "nspB "
                        + " " + mbps + "mbps " + " In:" + totalTestBytes + " Out:" + queuedBytes + " pct "
                        + (totalTestBytes / (float) queuedBytes) + " Messages:" + msgs + " Groups:" + grps); // Phrases/Clauses
                // Helps let us kill off the job.
                Thread.yield();
                System.gc();
            }

            // //////
            // reset the data to run the test again.
            // //////
            fastInput.reset();
            primitiveReader.reset();
            dynamicReader.reset(true);

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
    public void testDecodeGenVsInterp30000() {
        // /////////
        // ensure the generated code does the same thing as the interpreted
        // code.
        // plays both together and checks each as they are processed.
        // /////////
        FASTInput templateCatalogInput = new FASTInputByteArray(buildRawCatalogData());
        final TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(templateCatalogInput));

        // values which need to be set client side and are not in the template.
        catalog.setMessagePreambleSize((byte) 4);
        catalog.setMaxByteVectorLength(0, 0);// byte vectors are unused
        catalog.setMaxTextLength(14, 8);

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
        long totalTestBytes = sourceDataFile.length();

        FASTInputByteArray fastInput1 = buildInputForTestingByteArray(sourceDataFile);
        PrimitiveReader primitiveReader1 = new PrimitiveReader(fastInput1);
        FASTReaderDispatch readerDispatch1 = new FASTReaderDispatch(primitiveReader1, catalog.dictionaryFactory(),
                catalog.maxNonTemplatePMapSize(), catalog.dictionaryMembers(), catalog.getMaxTextLength(),
                catalog.getMaxByteVectorLength(), catalog.getTextGap(), catalog.getByteVectorGap(),
                catalog.fullScript(), catalog.getMaxGroupDepth(), 8, 7);
        FASTDynamicReader dynamicReader1 = new FASTDynamicReader(primitiveReader1, catalog, readerDispatch1);
        FASTRingBuffer queue1 = readerDispatch1.ringBuffer();

        FASTInputByteArray fastInput2 = buildInputForTestingByteArray(sourceDataFile);
        final PrimitiveReader primitiveReader2 = new PrimitiveReader(fastInput2);
        FASTReaderDispatch readerDispatch2 = new FASTReaderDispatchGenExample(primitiveReader2,
                catalog.dictionaryFactory(), catalog.maxNonTemplatePMapSize(), catalog.dictionaryMembers(),
                catalog.getMaxTextLength(), catalog.getMaxByteVectorLength(), catalog.getTextGap(),
                catalog.getByteVectorGap(), catalog.fullScript(), catalog.getMaxGroupDepth(), 8, 7);
        FASTDynamicReader dynamicReader2 = new FASTDynamicReader(primitiveReader2, catalog, readerDispatch2);
        FASTRingBuffer queue2 = readerDispatch2.ringBuffer();

        // final Map<Long,String> reads1 = new HashMap<Long,String>();
        // readerDispatch1.setDispatchObserver(new DispatchObserver(){
        //
        // @Override
        // public void tokenItem(long absPos, int token, int cursor, String
        // value) {
        // String msg =
        // "\n    R_"+TokenBuilder.tokenToString(token)+" id:"+(cursor>=catalog.scriptFieldIds.length?
        // "ERR": ""+catalog.scriptFieldIds[cursor])+" curs:"+cursor+
        // " tok:"+token+" "+value;
        // if (reads1.containsKey(absPos)) {
        // msg = reads1.get(absPos)+" "+msg;
        // }
        // reads1.put(absPos, msg);
        // }});

        // final Map<Long,String> reads2 = new HashMap<Long,String>();

        final int keep = 32;
        final int mask = keep - 1;
        final AtomicInteger idx = new AtomicInteger(0);
        final String[] reads2 = new String[keep];
        readerDispatch2.setDispatchObserver(new DispatchObserver() {

            @Override
            public void tokenItem(long absPos, int token, int cursor, String value) {
                String msg = " " + (primitiveReader2.totalRead() - primitiveReader2.bytesReadyToParse()) + " R_"
                        + TokenBuilder.tokenToString(token) + " id:"
                        + (cursor >= catalog.scriptFieldIds.length ? "ERR" : "" + catalog.scriptFieldIds[cursor])
                        + " curs:" + cursor + " tok:" + token + " " + value;

                reads2[mask & idx.incrementAndGet()] = msg.trim();
            }
        });

        int errCount = 0;
        int i = 0;
        while (dynamicReader1.hasMore() != 0 && dynamicReader2.hasMore() != 0) {

            while (queue1.hasContent() && queue2.hasContent()) {
                int int1 = queue1.readInteger(1);
                int int2 = queue2.readInteger(1);

                if (int1 != int2) {
                    errCount++;

                    if (errCount > 1) {

                        System.err.println("back up  " + queue1.contentRemaining() + " fixed spots in ring buffer");

                        int c = idx.get();
                        int j = keep;
                        while (--j >= 0) {
                            System.err.println(j + " " + reads2[mask & (c - j)]);
                        }
                        System.err.println("1:" + Integer.toBinaryString(int1));
                        System.err.println("2:" + Integer.toBinaryString(int2));

                        String msg = "int " + i + " byte " + (i * 4) + "  ";
                        // TODO: Z, regenerate code for this section that does
                        // not match.
                        // TODO: Z, skip pmap mismatch and look for real change
                        // all problems happen after this
                        // ASCIIOptional:001001/Default:000011/9 id:5799 curs:28
                        // tok:-1539571703

                        assertEquals(msg, int1, int2);
                    }
                }
                queue1.removeForward(1);
                queue2.removeForward(1);
                i++;
            }
        }
        assertEquals(primitiveReader1.totalRead(), primitiveReader2.totalRead());

    }

    @Test
    public void testDecodeEncodeComplex30000() {
        FASTInput templateCatalogInput = new FASTInputByteArray(buildRawCatalogData());
        final TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(templateCatalogInput));

        // values which need to be set client side and are not in the template.
        catalog.setMessagePreambleSize((byte) 4);
        catalog.setMaxByteVectorLength(0, 0);// byte vectors are unused
        catalog.setMaxTextLength(14, 8);

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
        long totalTestBytes = sourceDataFile.length();

        FASTInputByteArray fastInput = buildInputForTestingByteArray(sourceDataFile);

        // New memory mapped solution. No need to cache because we warm up and
        // OS already has it.
        // FASTInputByteBuffer fastInput =
        // buildInputForTestingByteBuffer(sourceDataFile);

        PrimitiveReader primitiveReader = new PrimitiveReader(fastInput);
        FASTReaderDispatch readerDispatch = new FASTReaderDispatchGenExample(primitiveReader,
                catalog.dictionaryFactory(), catalog.maxNonTemplatePMapSize(), catalog.dictionaryMembers(),
                catalog.getMaxTextLength(), catalog.getMaxByteVectorLength(), catalog.getTextGap(),
                catalog.getByteVectorGap(), catalog.fullScript(), catalog.getMaxGroupDepth(), 8, 7);
        FASTDynamicReader dynamicReader = new FASTDynamicReader(primitiveReader, catalog, readerDispatch);
        FASTRingBuffer queue = readerDispatch.ringBuffer();

        byte[] targetBuffer = new byte[(int) (totalTestBytes)];
        FASTOutputByteArray fastOutput = new FASTOutputByteArray(targetBuffer);

        // TODO: Z, force this error and add friendly message, when minimize
        // latency set to false these need to be much bigger?
        int writeBuffer = 2048;
        int maxGroupCount = 3;// NOTE: may need to be VERY large if minimize
                              // latency is turned off!!
        PrimitiveWriter primitiveWriter = new PrimitiveWriter(writeBuffer, fastOutput, maxGroupCount, true);
        FASTWriterDispatch writerDispatch = new FASTWriterDispatch(primitiveWriter, catalog.dictionaryFactory(),
                catalog.templatesCount(), catalog.getMaxTextLength(), catalog.getMaxByteVectorLength(),
                catalog.getTextGap(), catalog.getByteVectorGap(), queue, catalog.maxNonTemplatePMapSize(),
                catalog.dictionaryMembers(), catalog.fullScript(), catalog.getMaxGroupDepth());

        FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(primitiveWriter, catalog, queue, writerDispatch);

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
        int count = 256;

        long wroteSize = 0;
        int msgs = 0;
        int grps = 0;
        int iter = warmup;
        while (--iter >= 0) {
            msgs = 0;
            grps = 0;
            int flags = 0; // same id needed for writer construction
            while (0 != (flags = dynamicReader.hasMore())) {
                Thread.yield();
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
            primitiveReader.reset();
            dynamicReader.reset(true);

            primitiveWriter.flush();
            wroteSize = Math.max(wroteSize, primitiveWriter.totalWritten());
            fastOutput.reset();
            primitiveWriter.reset();
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
            while (0 != dynamicReader.hasMore()) {
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
            primitiveReader.reset();
            dynamicReader.reset(true);

            fastOutput.reset();
            primitiveWriter.reset();
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

    private FASTInputByteArray buildInputForTestingByteArray(File fileSource) {
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

    private String hexString(byte[] targetBuffer) {
        StringBuilder builder = new StringBuilder();

        for (byte b : targetBuffer) {

            String tmp = Integer.toHexString(0xFF & b);
            builder.append(tmp.substring(Math.max(0, tmp.length() - 2))).append(" ");

        }
        return builder.toString();
    }

    private String binString(byte[] targetBuffer) {
        StringBuilder builder = new StringBuilder();

        for (byte b : targetBuffer) {

            String tmp = Integer.toBinaryString(0xFF & b);
            builder.append(tmp.substring(Math.max(0, tmp.length() - 8))).append(" ");

        }
        return builder.toString();
    }

    private byte[] buildRawCatalogData() {
        URL source = getClass().getResource("/performance/example.xml");

        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        File fileSource = new File(source.getFile().replace("%20", " "));
        try {
            TemplateLoader.buildCatalog(catalogBuffer, fileSource);
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }

        assertTrue("Catalog must be built.", catalogBuffer.size() > 0);

        byte[] catalogByteArray = catalogBuffer.toByteArray();
        return catalogByteArray;
    }

}
