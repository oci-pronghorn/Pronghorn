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
         final boolean single = false;//true;//false;
                
                  
         //TODO: for multi test we really need to have it writing to multiple ring buffers.
          byte[] catBytes = buildRawCatalogData(clientConfig, templateSource);

          TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes); 
          int maxPMapCountInBytes = TemplateCatalogConfig.maxPMapCountInBytes(catalog);             
          
          
          FASTClassLoader.deleteFiles();
         FASTDecoder readerDispatch = DispatchLoader.loadDispatchReader(catBytes);
       
       //  FASTDecoder readerDispatch = new FASTReaderInterpreterDispatch(catBytes); 
          
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
                  case -1://end of file
                      ok = false;
                      break;
                  case 0: //no room to read
                      break;
                  case 1: //read one fragment
                      FASTRingBuffer rb = RingBuffers.get(ringBuffers,bufId);                      
                      FASTRingBuffer.moveNext(rb);
                      
                      if (rb.isNewMessage) {
                          msgs.incrementAndGet();
                      }
                      
                      //your usage of these fields would go here. 
                                            
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
    
    
  
    
    
    public int templateId;
    public int preamble;
    
    private double multiThreadedExample(FASTDecoder readerDispatch, final AtomicInteger msgs, FASTInputReactor reactor, PrimitiveReader reader) {

        FASTRingBuffer[] buffers = RingBuffers.buffers(readerDispatch.ringBuffers);
        
        int reactors = 1;
        ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(reactors+buffers.length); 

        
        double start = System.nanoTime();
          
        final AtomicBoolean isAlive = reactor.start(executor, reader);
        
        int b = buffers.length;
        while (--b>=0) {
            final FASTRingBuffer rb = buffers[b];
            Runnable run = new Runnable() {
                char[] temp = new char[64];
                
                
                @Override
                public void run() {
                    int totalMessages = 0;
                    do {
                        
                        if (FASTRingBuffer.moveNext(rb)) { //if we do this too fast we get
//                            Exception in thread "pool-14-thread-2" java.lang.ArrayIndexOutOfBoundsException: 10
//                            at com.ociweb.jfast.stream.FASTRingBuffer.sequenceLengthDetector(FASTRingBuffer.java:287)
//                            at com.ociweb.jfast.stream.FASTRingBuffer.beginFragment(FASTRingBuffer.java:184)
//                            at com.ociweb.jfast.stream.FASTRingBuffer.moveNext(FASTRingBuffer.java:169)
//                            at com.ociweb.jfast.Test$1.run(Test.java:176)
//                            at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
//                            at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
//                            at java.lang.Thread.run(Thread.java:744)
                            
                            if (rb.isNewMessage) {
                                totalMessages++;
                            //    processMessage(temp, rb);                          
                                
                            }
                        }
                        //Thread.yield(); //add this back in to expose other bugs.
                        
                      //dump the data as fast as possible, this is faster than the single 
//                      rb.workingTailPos.value = rb.headPos.longValue();
//                      rb.tailPos.lazySet(rb.workingTailPos.value); 
//                      Thread.yield();
                        
                    } while (isAlive.get());
                    msgs.addAndGet(totalMessages);                    
                }
                
            };
            executor.execute(run);
        }
        
        
        executor.shutdown();
        try {
            executor.awaitTermination(1,TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        double duration = System.nanoTime() - start;
       // System.err.println("finished one test");
        executor.shutdownNow();
        return duration;
    }
    //TODO: C, need test for optional groups this is probably broken. 

    
    
    
    private void processMessage(char[] temp, FASTRingBuffer rb) {
       

        //final int IDX_AppVerId = rb.from.lookupIDX("ApplVerID");
        
        templateId = FASTRingBufferReader.readInt(rb, 0);
        preamble = FASTRingBufferReader.readInt(rb, 1);
                 
                 switch (rb.messageId) {
          case 1:
              int len = FASTRingBufferReader.readDataLength(rb, 2);
              FASTRingBufferReader.readASCII(rb, 2, temp, 0);
             // System.err.println("ApplVerID: "+new String(temp,0,len));
              
              
              len = FASTRingBufferReader.readDataLength(rb, 4);
              FASTRingBufferReader.readASCII(rb, 4, temp, 0);                                    
             // System.err.println("MessageType: "+new String(temp,0,len));
              
              len = FASTRingBufferReader.readDataLength(rb, 6);
              FASTRingBufferReader.readASCII(rb, 6, temp, 0);                                    
              //System.err.println("SenderCompID: "+new String(temp,0,len));
              
              int msgSeqNum = FASTRingBufferReader.readInt(rb, 8);
              int sendingTime = FASTRingBufferReader.readInt(rb, 9);
              int tradeDate = FASTRingBufferReader.readInt(rb, 10);
              int seqCount = FASTRingBufferReader.readInt(rb, 11);
              //System.err.println(sendingTime+" "+tradeDate+" "+seqCount);
              while (--seqCount>=0) {
                  while(!FASTRingBuffer.moveNext(rb)) { //keep calling if we have no data? 
                  };
                  int mDUpdateAction = FASTRingBufferReader.readInt(rb, 0);
                  int mDPriceLevel = FASTRingBufferReader.readInt(rb, 1);
                  
                  FASTRingBufferReader.readASCII(rb, 2, temp, 0); 
                  
                  int openCloseSettleFlag = FASTRingBufferReader.readInt(rb, 4);
                  int securityIDSource = FASTRingBufferReader.readInt(rb, 5);
                  int securityID = FASTRingBufferReader.readInt(rb, 6);
                  int rptSeq = FASTRingBufferReader.readInt(rb, 7);
                  //MDEntryPx
                  int mDEntryPxExpo = FASTRingBufferReader.readDecimalExponent(rb, 8);
                  long mDEntrypxMant = FASTRingBufferReader.readDecimalExponent(rb, 8);
                  int mDEntryTime = FASTRingBufferReader.readInt(rb, 11);
                  int mDEntrySize = FASTRingBufferReader.readInt(rb, 12);
                  int numberOfOrders = FASTRingBufferReader.readInt(rb, 13);
                  
                  FASTRingBufferReader.readASCII(rb, 14, temp, 0); //TradingSessionID
                  
                  
                  
                 // System.err.println(mDUpdateAction);
                  
               //TODO: write this out to a binary file?   
                  
              }
              
              
              break;
          case 2:
              
              
              break;
          case 99:
              
              len = FASTRingBufferReader.readDataLength(rb, 2);
              FASTRingBufferReader.readASCII(rb, 2, temp, 0);
//                                          rb.tailPos.lazySet(rb.workingTailPos.value);
              //System.err.println("MessageType: "+new String(temp,0,len));
              
              break;
          default:
              System.err.println("Did not expect "+rb.messageId);
                 }
    }

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

   
    
    
}
