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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.generator.GeneratorUtils;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArrayEquals;
import com.ociweb.jfast.stream.DispatchObserver;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTInputReactor;
import com.ociweb.jfast.stream.FASTListener;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferReader;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;
import com.ociweb.jfast.stream.RingBuffers;
import com.ociweb.jfast.util.Profile;
import com.ociweb.jfast.util.Stats;

public class TemplateLoaderTest {


    
    
    @Test
    public void buildRawCatalog() {

        byte[] catalogByteArray = buildRawCatalogData();
        assertEquals(707, catalogByteArray.length);
               
        
        // reconstruct Catalog object from stream
        TemplateCatalogConfig catalog = new TemplateCatalogConfig(catalogByteArray);

        boolean ok = false;
        int[] script = null;
        try {
            // /performance/example.xml contains 3 templates.
            assertEquals(3, catalog.templatesCount());

            script = catalog.fullScript();
            assertEquals(54, script.length);
            assertEquals(TypeMask.Group, TokenBuilder.extractType(script[0]));// First
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
        final TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes); 

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
        long totalTestBytes = sourceDataFile.length();

        int maxPMapCountInBytes = 2 + ((Math.max(
                catalog.maxTemplatePMapSize(), catalog.maxNonTemplatePMapSize()) + 2) * catalog.getMaxGroupDepth());

      
        PrimitiveReader reader = new PrimitiveReader(buildInputArrayForTesting(sourceDataFile), maxPMapCountInBytes);
        
        FASTClassLoader.deleteFiles();
        
        FASTDecoder readerDispatch = DispatchLoader.loadDispatchReader(catBytes);
    //    FASTDecoder readerDispatch = new FASTReaderInterpreterDispatch(catBytes);//not using compiled code
        

        Stats stats = new Stats(100000,13000000,1000000,100000000);    
        
        
        System.err.println("using: "+readerDispatch.getClass().getSimpleName());
        System.gc();
        
        FASTRingBuffer queue = RingBuffers.get(readerDispatch.ringBuffers,0);      

        int warmup = 256;
        int count = 1024;
        final int[] fullScript = catalog.getScriptTokens();
        
        
        final byte[] preamble = new byte[catalog.clientConfig.getPreableBytes()];

        final AtomicInteger msgs = new AtomicInteger();
        int frags = 0;      
        
        final AtomicLong totalBytesOut = new AtomicLong();
        final AtomicLong totalRingInts = new AtomicLong();

        
        FASTInputReactor reactor=null;

        
        int iter = warmup;
        while (--iter >= 0) {
            msgs.set(0);
            frags = 0;

            reactor = new FASTInputReactor(readerDispatch,reader);
            FASTRingBuffer rb = RingBuffers.get(readerDispatch.ringBuffers,0);
            rb.reset();

            while (FASTInputReactor.pump(reactor)>=0) { //continue if there is no room or if a fragment is read.
                FASTRingBuffer.moveNext(rb);

                frags++;
                if (rb.isNewMessage) {
                    int templateId = rb.messageId;
                    
                    msgs.incrementAndGet();
                    

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

                   // int templateId2 = FASTRingBufferReader.readInt(queue, bufferIdx);
                    bufferIdx += 1;// point to first field
                    assertTrue("found " + templateId, 1 == templateId || 2 == templateId || 99 == templateId);

                    int i = catalog.getTemplateStartIdx()[templateId];
                    int limit = catalog.getTemplateLimitIdx()[templateId];
                    // System.err.println("new templateId "+templateId);
                    while (i < limit) {
                        int token = fullScript[i++];
                        // System.err.println("xxx:"+bufferIdx+" "+TokenBuilder.tokenToString(token));

                        if (isText(token)) {
                            totalBytesOut.addAndGet(4 * FASTRingBufferReader.readDataLength(queue, bufferIdx));
                        }

                        // find the next index after this token.
                        int fSize = TypeMask.ringBufferFieldSize[TokenBuilder.extractType(token)];
                        if (!GeneratorUtils.WRITE_CONST && !TokenBuilder.isOptional(token) && TokenBuilder.extractOper(token)==OperatorMask.Field_Constant) {
                            fSize = 0; //constants are not written
                        }
                        bufferIdx += fSize;

                    }
                    totalBytesOut.addAndGet(4 * bufferIdx);
                    totalRingInts.addAndGet(bufferIdx);

                    // must dump values in buffer or we will hang when reading.
                    // only dump at end of template not end of sequence.
                    // the removePosition must remain at the beginning until
                    // message is complete.
                    
                    //NOTE: MUST NOT DUMP IN THE MIDDLE OF THIS LOOP OR THE PROCESSING GETS OFF TRACK
                    //FASTRingBuffer.dump(queue);
                //    rb.tailPos.lazySet(rb.workingTailPos.value);
                }
            }
            
            rb.reset();
            
            
            //fastInput.reset();
            PrimitiveReader.reset(reader);
            readerDispatch.sequenceCountStackHead = -1;            
            RingBuffers.reset(readerDispatch.ringBuffers);
                        
  //          System.err.println(reactor.stats.toString()+" ns");
            
        }
        if (warmup>0) {
            totalBytesOut.set(totalBytesOut.longValue()/warmup);
            totalRingInts.set(totalRingInts.longValue()/warmup);
        }
        
        Profile.start();
        
        iter = count+warmup;
        while (--iter >= 0) {
            if (Thread.interrupted()) {
                System.exit(0);
            }
            
            reactor = new FASTInputReactor(readerDispatch,reader);
            
            FASTRingBuffer rb = null; 
            rb =  RingBuffers.get(readerDispatch.ringBuffers,0);
            rb.reset();
            double duration = 0;
            
            try{
                double start = System.nanoTime();
    
                //Preload the ringBuffer with a few pumps to ensure we
                //are not testing against an always empty buffer.
                int few = 4;
                while (--few>=0) {
                    FASTInputReactor.pump(reactor);
                }               
                while (FASTInputReactor.pump(reactor)>=0) { //72-88
                 //   FASTRingBuffer.dump(rb);
                    //int tmp = Profile.version.get();
                    while (FASTRingBuffer.moveNext(rb)) {
                       // rb.tailPos.lazySet(rb.workingTailPos.value);
                    }; //11
                    //Profile.count += (Profile.version.get()-tmp);
                }
                //the buffer has extra records in it so we must clean them out here.
                while (FASTRingBuffer.moveNext(rb)) {
                     
                   // rb.tailPos.lazySet(rb.workingTailPos.value);
                }
                
                duration = System.nanoTime() - start;
            } catch (Throwable ie) {
               ie.printStackTrace();
               System.exit(0);
            }
            if (iter<count) {
                stats.sample((long)duration);
                
                if ((0x7F & iter) == 0) {
                    int ns = (int) stats.valueAtPercent(.60);//duration;
                    float mmsgPerSec = (msgs.intValue() * (float) 1000l / ns);
                    float nsPerByte = (ns / (float) totalTestBytes);
                    int mbps = (int) ((1000l * totalTestBytes * 8l) / ns);
                    
                    float mfieldPerSec = (totalRingInts.longValue()* (float) 1000l / ns);
    
                    System.err.println("Duration:" + ns + "ns " + " " + mmsgPerSec + "MM/s " + " " + nsPerByte + "nspB "
                            + " " + mbps + "mbps " + " In:" + totalTestBytes + " Out:" + totalBytesOut + " cmpr:"
                            + (1f-(totalTestBytes / (float) totalBytesOut.longValue())) + " Messages:" + msgs + " Frags:" + frags
                            + " RingInts:"+totalRingInts+ " mfps "+mfieldPerSec 
                            ); // Phrases/Clauses
                    // Helps let us kill off the job.
                }
            }

            // //////
            // reset the data to run the test again.
            // //////
            //fastInput.reset();
            PrimitiveReader.reset(reader);
            readerDispatch.reset(catalog.dictionaryFactory());
            
   //         rb.tailPos.lazySet(rb.workingTailPos.value);

        }
        System.err.println(stats.toString()+" ns  total:"+stats.total());
        
        System.err.println(Profile.results());

        
    }

    private boolean isText(int token) {
        return 0x08 == (0x1F & (token >>> TokenBuilder.SHIFT_TYPE));
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
        final TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
        int maxPMapCountInBytes = TemplateCatalogConfig.maxPMapCountInBytes(catalog);   

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
        long totalTestBytes = sourceDataFile.length();
        final byte[] testBytesData = buildInputArrayForTesting(sourceDataFile);

        FASTInputByteArray fastInput = new FASTInputByteArray(testBytesData);

        // New memory mapped solution. No need to cache because we warm up and
        // OS already has it.
        // FASTInputByteBuffer fastInput =
        // buildInputForTestingByteBuffer(sourceDataFile);

        PrimitiveReader reader = new PrimitiveReader(2048, fastInput, maxPMapCountInBytes);
        
        FASTDecoder readerDispatch = DispatchLoader.loadDispatchReader(catBytes); 
       // readerDispatch = new FASTReaderInterpreterDispatch(catBytes);//not using compiled code
        System.err.println("using: "+readerDispatch.getClass().getSimpleName());
        
        final AtomicInteger msgs = new AtomicInteger();
        
        FASTInputReactor reactor = new FASTInputReactor(readerDispatch,reader);
        
        FASTRingBuffer queue = RingBuffers.get(readerDispatch.ringBuffers,0);

        FASTOutputByteArrayEquals fastOutput = new FASTOutputByteArrayEquals(testBytesData,queue.from.tokens);

        // TODO: Z, force this error and add friendly message, when minimize
        // latency set to false these need to be much bigger?
        int writeBuffer = 2048;
        
        int maxGroupCount = catalog.getScriptTokens().length; //overkill but its fine for testing. 
        // NOTE: may need to be VERY large if minimize
        // latency is turned off!!
        
        PrimitiveWriter writer = new PrimitiveWriter(writeBuffer, fastOutput, maxGroupCount, true);
        
        //unusual case just for checking performance. Normally one could not pass the catalog.ringBuffer() in like this.        
        FASTEncoder writerDispatch = new FASTWriterInterpreterDispatch(catalog, readerDispatch.ringBuffers);
 //       FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriter(catBytes); 
        System.err.println("using: "+writerDispatch.getClass().getSimpleName());

        FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(writer, queue, writerDispatch);

        System.gc();
        
        int warmup = 20;// set much larger for profiler
        int count = 128;
        

        long wroteSize = 0;
        msgs.set(0);
        int grps = 0;
        int iter = warmup;
        while (--iter >= 0) {
            msgs.set(0);
            grps = 0;
            DictionaryFactory dictionaryFactory = writerDispatch.dictionaryFactory;
            
            dictionaryFactory.reset(writerDispatch.intValues);
            dictionaryFactory.reset(writerDispatch.longValues);
            dictionaryFactory.reset(writerDispatch.byteHeap);
            while (FASTInputReactor.pump(reactor)>=0) { //continue if there is no room or a fragment is read
   
                    FASTRingBuffer.moveNext(queue);
                    if (queue.messageId>=0) { //skip if we are waiting for more content.
                        
                        if (queue.isNewMessage) {
                            msgs.incrementAndGet();
                        }
                        
                        //TODO: A, writer needs field access api? but not here because the fields are already in the right place in the ring buffer. Need to show ring buffer copy.
                        
                        
                        try{   //TODO: A, writer needs to be comipled
                             dynamicWriter.write();
                            } catch (FASTException e) {
                                System.err.println("ERROR: cursor at "+writerDispatch.getActiveScriptCursor()+" "+TokenBuilder.tokenToString(queue.from.tokens[writerDispatch.getActiveScriptCursor()]));
                                throw e;
                            }                            
                        grps++;
                   }

            }
            

            queue.reset();

            fastInput.reset();
            PrimitiveReader.reset(reader);
            readerDispatch.reset(catalog.dictionaryFactory());

            PrimitiveWriter.flush(writer);
            wroteSize = Math.max(wroteSize, PrimitiveWriter.totalWritten(writer));
            fastOutput.reset();
            PrimitiveWriter.reset(writer);
            dynamicWriter.reset(true);

        }

        // Expected total read fields:2126101
        assertEquals("test file bytes", totalTestBytes, wroteSize);

        iter = count;
        while (--iter >= 0) {

            DictionaryFactory dictionaryFactory = writerDispatch.dictionaryFactory;
            dictionaryFactory.reset(writerDispatch.intValues);
            dictionaryFactory.reset(writerDispatch.longValues);
            dictionaryFactory.reset(writerDispatch.byteHeap);
            double start = System.nanoTime();
            
            while (FASTInputReactor.pump(reactor)>=0) {  
                    FASTRingBuffer.moveNext(queue);
                    if (queue.messageId>=0) { //skip if we are waiting for more content.
                            dynamicWriter.write();  
                   }
            }
            
            
            double duration = System.nanoTime() - start;

            if ((0x3F & iter) == 0) {
                int ns = (int) duration;
                float mmsgPerSec = (msgs.intValue() * (float) 1000l / ns);
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
            readerDispatch.reset(catalog.dictionaryFactory());

            fastOutput.reset();
            PrimitiveWriter.reset(writer);
            dynamicWriter.reset(true);

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

    static byte[] buildInputArrayForTesting(File fileSource) {
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
        ClientConfig clientConfig = new ClientConfig();
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
