package com.ociweb.jfast;

import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.loader.ClientConfig;
import com.ociweb.jfast.loader.FieldReferenceOffsetManager;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.loader.TemplateLoader;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTInputReactor;
import com.ociweb.jfast.stream.FASTListener;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferReader;
import com.ociweb.jfast.stream.RingBuffers;
import com.ociweb.jfast.stream.FASTRingBuffer.PaddedLong;

public class Test {

    public static void main(String[] args) {
        
        //this example uses the preamble feature
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setPreableBytes((short)4);
        String templateSource = "/performance/example.xml";
        String dataSource = "/performance/complex30000.dat";
        
        new Test().decode(clientConfig, templateSource, dataSource);
    }
    
    public void decode(ClientConfig clientConfig, String templateSource, String dataSource) {
         final int count = 1024000;
         final boolean single = false;
                
                  
          byte[] catBytes = buildRawCatalogData(clientConfig, templateSource);

          TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes); 
          int maxPMapCountInBytes = TemplateCatalogConfig.maxPMapCountInBytes(catalog);             
          
          
          FASTClassLoader.deleteFiles();
         FASTDecoder readerDispatch = DispatchLoader.loadDispatchReader(catBytes);
       
        // FASTDecoder readerDispatch = new FASTReaderInterpreterDispatch(catBytes); 
          
          final AtomicInteger msgs = new AtomicInteger();
          
                  
          int queuedBytes = 10;

          int iter = count;
          while (--iter >= 0) {
              FASTInputStream fastInputStream = new FASTInputStream(testDataInputStream(dataSource));
              PrimitiveReader reader = new PrimitiveReader(1024*1024*2,fastInputStream, maxPMapCountInBytes);
              
              PrimitiveReader.fetch(reader);//Pre-load the file so we only count the parse time.
                            
              FASTInputReactor reactor = new FASTInputReactor(readerDispatch, reader);
                            
              msgs.set(0);                         
              
              
              double duration = single ?
                                singleThreadedExample(readerDispatch, msgs, reactor) :
                                multiThreadedExample(readerDispatch, msgs, reactor, reader);
              
                            
              if (shouldPrint(iter)) {
                  printSummary(msgs.get(), queuedBytes, duration, fastInputStream.totalBytes()); 
              }

              //reset the dictionary to run the test again.
              readerDispatch.reset(catalog.dictionaryFactory());

          }

      }

    private double singleThreadedExample(FASTDecoder readerDispatch, final AtomicInteger msgs, FASTInputReactor reactor) {
        
        double start = System.nanoTime();
          
          /////////////////////////////////////
          //Example of single threaded usage
          /////////////////////////////////////
          RingBuffers ringBuffers = readerDispatch.ringBuffers;
          boolean ok = true;
          int bufId;
          while (ok) {
              switch (bufId = FASTInputReactor.pump(reactor)) {
                  case -1:
                      ok = false;
                      break;
                  default:
                      FASTRingBuffer rb = RingBuffers.get(ringBuffers,bufId);                      
                      FASTRingBuffer.moveNext(rb);
                      
                      if (rb.isNewMessage) {
                          msgs.incrementAndGet();
                      }
                      
                      //your usage of these fields would go here. 
                      
                      rb.tailPos.lazySet(rb.workingTailPos.value);
                      
                      break;
              }
              
          }
          //System.err.println(".");
          //////////////////////////////////
          //End of single threaded example
          //////////////////////////////////
          
          double duration = System.nanoTime() - start;
          return duration;
    }
    
    
    
//    private double multiThreadedExample2(FASTDecoder readerDispatch, final AtomicInteger msgs, FASTInputReactor reactor) {
//        
//        
//        
//        Runnable reader = new Runnable() {
//
//            @Override
//            public void run() {
//
//                
//                
//                
//                
//                    int i = testSize;
//                    
//                    AtomicLong hp = rb.headPos;
//                    AtomicLong tp = rb.tailPos;
//                    
//                    long headPosCache = hp.longValue();
//                    long targetHead =  granularity+tp.longValue();
//                    PaddedLong wrkTlPos = rb.workingTailPos;
//                    
//                    while (--i>=0) {
//                        int j = messageSize;
//                        
//                        //wait for at least n messages to be available 
//                        //waiting for headPos to change
//
//                        headPosCache = FASTRingBuffer.spinBlock(hp, headPosCache, targetHead);
//                        
//                        
//                        //read the record
//                        while (--j>=0) {
//                            
//                            //int value = FASTRingBufferReader.readInt(rb, 0); //read from workingTailPosition + 0
//                            int value = FASTRingBufferReader.readInt(rbB, rbMask, wrkTlPos, 0);
//                            if (value!=j) {                                
//                                fail("expected "+j+" but found "+i);
//                            }   
//                            wrkTlPos.value++; //TODO: C, reader has external inc but writer has internal inc
//                            
//                        }
//                        
//                        //allow writer to write up to new tail position
//                        if (0==(i&rb.chunkMask) ) {  
//                            targetHead = FASTRingBuffer.releaseRecords(granularity, tp, wrkTlPos);
//                        }
//                        
//                    }
//                
//            }
//            
//        };
//        new Thread(reader).start();
//        
//        
//        long start = System.nanoTime();
//        
//        AtomicLong hp = rb.headPos;
//        AtomicLong tp = rb.tailPos;
//        
//        int i = testSize;
//        long tailPosCache = tp.get();
//        long targetPos = hp.longValue()+granularity-rb.maxSize;
//        PaddedLong wrkHdPos = rb.workingHeadPos;
//        while (--i>=0) {
//            
//            int j = messageSize;
//            //wait for room to fit one message
//            //waiting on the tailPosition to move the others are constant for this scope.
//            //workingHeadPositoin is same or greater than headPosition
//            
//            tailPosCache = FASTRingBuffer.spinBlock(tp, tailPosCache, targetPos);
//
//            
//            //write the record
//            while (--j>=0) {                    
//                FASTRingBuffer.addValue(rbB, rbMask, wrkHdPos, j);  
//            }
//            //allow read thread to read up to new head position
//            if (0==(i&rb.chunkMask) ) {
//                targetPos = FASTRingBuffer.releaseRecords(granularity, hp, wrkHdPos);
//                targetPos -= rb.maxSize;
//            }
//        }
//        tailPosCache = FASTRingBuffer.spinBlock(tp, tailPosCache, hp.longValue());
//
//        return System.nanoTime()-start;
//        
//        
//
//    }
//    
    
    
    public int templateId;
    public int preamble;
    
    private double multiThreadedExample(FASTDecoder readerDispatch, final AtomicInteger msgs, FASTInputReactor reactor, PrimitiveReader reader) {
      //  System.err.println("start");
        ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(1);//number of reactors to support 

        
        char[] temp = new char[64];
        
        double start = System.nanoTime();
          
        AtomicBoolean isAlive = reactor.start(executor, reader);
        
        int messageSize = 50;
        
        FASTRingBuffer rb = RingBuffers.get(readerDispatch.ringBuffers,0);
        AtomicLong hp = rb.headPos;
        AtomicLong tp = rb.tailPos;
        
        long headPosCache = hp.longValue();
        PaddedLong wrkTlPos = rb.workingTailPos;
        final int granularity = 30;//messageSize<<rb.chunkBits;
        long targetHead =  granularity+tp.longValue();
        
         final int IDX_AppVerId = rb.from.lookupIDX("ApplVerID");
         
        // rb.tailPos.lazySet(rb.workingTailPos.value);
         
         
         
         int j = 0;
        
            do {    
                //wait until data is written and released up to the new target
                long lastCheckedValue = headPosCache;
                while ( lastCheckedValue < targetHead && isAlive.get()) {  //was spinLock but it must watch is Alive!!
                    //NOTE: when used with more threads/jobs than cores may want to Thread.yield() here.
                    lastCheckedValue = hp.longValue();
                }
                headPosCache = lastCheckedValue;
                //assign work value to head like we read up to it
                //set tail position to the new work position to let writer continue
                if (FASTRingBuffer.moveNext(rb)) {//If queue backs up this move next will have problems!
                    if (rb.isNewMessage) {
                        msgs.incrementAndGet();
                    }
                };
//TODO: must confirm that the writer stops when this reader is not reading.
                
             //   if ((++j&0xF)==0 ) {
                    //wrkTlPos.value = hp.longValue();
                    targetHead = FASTRingBuffer.releaseRecords(granularity, tp, wrkTlPos);
           //     }
                //dump the data as fast as possible
//                if ((++j&0xFFF)==0) {
//                    wrkTlPos.value = hp.longValue();
//                    tp.lazySet(wrkTlPos.value); 
//                }
                
//                //System.err.println("enter spint lock");
//                long lastCheckedValue = headPosCache;
//                while ( lastCheckedValue < targetHead && isAlive.get()) {  
//                    //NOTE: when used with more threads/jobs than cores may want to Thread.yield() here.
//                    lastCheckedValue = hp.longValue();
//                }
//                headPosCache = lastCheckedValue;
//                //System.err.println("exit spint lock");
//                   
//                    //move to next fragment to read
//                    while (FASTRingBuffer.moveNext(rb)) {
//                        
//                        if (rb.isNewMessage) {
//                            msgs.incrementAndGet();
//                        }
//                        
//                        if (0==(j&0x01) || !isAlive.get() ) {  
//                            tp.lazySet(wrkTlPos.value); //we are only looking for space for 1 but wrote n!! records!
//                            targetHead = wrkTlPos.value + granularity;
//                        }
//
//                        
//                        j++;
//                    
//
//                    }                         
                    
            } while (isAlive.get()); //TODO: A, this boolean for end of test is slowing us down!!
            
          
        double duration = System.nanoTime() - start;
       // System.err.println("finished one test");
        executor.shutdownNow();
        return duration;
    }
    //TODO: C, need test for optional groups this is probably broken. 

    private boolean shouldPrint(int iter) {
        return (0x7F & iter) == 0;
    }

    private void printSummary(int msgs, int queuedBytes, double duration, long totalTestBytes) {
        int ns = (int) duration;
          float mmsgPerSec = (msgs * (float) 1000l / ns);
          float nsPerByte = (ns / (float) totalTestBytes);
          int mbps = (int) ((1000l * totalTestBytes * 8l) / ns);

          System.err.println("Duration:" + ns + "ns " + " " + mmsgPerSec + "MM/s " + " " + nsPerByte + "nspB "
                  + " " + mbps + "mbps " + " In:" + totalTestBytes + " Out:" + queuedBytes + " pct "
                  + (totalTestBytes / (float) queuedBytes) + " Messages:" + msgs);
    }
    
    static FASTInputByteArray buildInputForTestingByteArray(File fileSource) {
        byte[] fileData = null;
        try {
            // do not want to time file access so copy file to memory
            fileData = new byte[(int) fileSource.length()];
            FileInputStream inputStream = new FileInputStream(fileSource);
            int readBytes = inputStream.read(fileData);
            inputStream.close();
            assert(fileData.length==readBytes);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        FASTInputByteArray fastInput = new FASTInputByteArray(fileData);
        return fastInput;
    }
    
    private static InputStream testDataInputStream(String resource) {
        
        InputStream resourceInput = Test.class.getResourceAsStream(resource);
        if (null!=resourceInput) {
            return resourceInput;            
        }
        
        try {
            return new FileInputStream(new File(resource));
        } catch (FileNotFoundException e) {
            throw new FASTException(e);
        }
    }

    private static byte[] buildRawCatalogData(ClientConfig clientConfig, String source) {


        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            TemplateLoader.buildCatalog(catalogBuffer, source, clientConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assert(catalogBuffer.size() > 0);

        byte[] catalogByteArray = catalogBuffer.toByteArray();
        return catalogByteArray;
    }

    
//  if (rb.isNewMessage) {
//  
//  templateId = FASTRingBufferReader.readInt(rb, 0);
//  preamble = FASTRingBufferReader.readInt(rb, 1);
//  
//  switch (rb.messageId) {
//      case 1:
//          int len = FASTRingBufferReader.readDataLength(rb, 2);
//          FASTRingBufferReader.readASCII(rb, 2, temp, 0);
//         // System.err.println("ApplVerID: "+new String(temp,0,len));
//          
//          
//          len = FASTRingBufferReader.readDataLength(rb, 4);
//          FASTRingBufferReader.readASCII(rb, 4, temp, 0);                                    
//         // System.err.println("MessageType: "+new String(temp,0,len));
//          
//          len = FASTRingBufferReader.readDataLength(rb, 6);
//          FASTRingBufferReader.readASCII(rb, 6, temp, 0);                                    
//          //System.err.println("SenderCompID: "+new String(temp,0,len));
//          
//          int msgSeqNum = FASTRingBufferReader.readInt(rb, 8);
//          int sendingTime = FASTRingBufferReader.readInt(rb, 9);
//          int tradeDate = FASTRingBufferReader.readInt(rb, 10);
//          int seqCount = FASTRingBufferReader.readInt(rb, 11);
//          //System.err.println(sendingTime+" "+tradeDate+" "+seqCount);
//          while (--seqCount>=0) {
//              while(!FASTRingBuffer.moveNext(rb)) { //keep calling if we have no data? 
//              };
//              rb.tailPos.lazySet(rb.workingTailPos.value);
//              int mDUpdateAction = FASTRingBufferReader.readInt(rb, 0);
//             // System.err.println(mDUpdateAction);
//              
//           //TODO: write this out to a binary file?   
//              
//          }
//          
//          
//          //
////             rb.tailPos.lazySet(rb.workingTailPos.value);
//          break;
//      case 2:
//          
////              rb.tailPos.lazySet(rb.workingTailPos.value);
//          break;
//      case 99:
//          
//          len = FASTRingBufferReader.readDataLength(rb, 2);
//          FASTRingBufferReader.readASCII(rb, 2, temp, 0);
////                      rb.tailPos.lazySet(rb.workingTailPos.value);
//          //System.err.println("MessageType: "+new String(temp,0,len));
//          
//          break;
//      default:
//          System.err.println("Did not expect "+rb.messageId);
//  }                     
//  
////  if ((++j&0xFF)==0) {
////  
////  }
//} else {
////            rb.tailPos.lazySet(rb.workingTailPos.value);
//}
    
    
}
