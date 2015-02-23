package com.ociweb.jfast.benchmark;

import static com.ociweb.pronghorn.ring.RingReader.eqASCII;
import static com.ociweb.pronghorn.ring.RingReader.readASCII;
import static com.ociweb.pronghorn.ring.RingReader.readDataLength;
import static com.ociweb.pronghorn.ring.RingReader.readDecimalExponent;
import static com.ociweb.pronghorn.ring.RingReader.readDecimalMantissa;
import static com.ociweb.pronghorn.ring.RingReader.readInt;
import static com.ociweb.pronghorn.ring.RingReader.readLong;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.generator.GeneratorUtils;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTReaderReactor;

public class ThreadingTest {

	
	@Ignore //Still in work needs to assert all the right stuff.
	public void builtInTest() {
		//this example uses the preamble feature
        //large value for bandwidth, small for latency
        ClientConfig clientConfig = new ClientConfig(20,22);
        clientConfig.setPreableBytes((short)4);
        String templateSource = "/performance/example.xml";
        String dataSource = "/performance/complex30000.dat";
        boolean singleThreaded = true;
        
        decode(clientConfig, templateSource, dataSource, singleThreaded);
	}
    
    public void decode(ClientConfig clientConfig, String templateSource, String dataSource, boolean single) {
         final int count = 128;
                
                  
         //TODO: for multi test we really need to have it writing to multiple ring buffers.
          byte[] catBytes = buildRawCatalogData(clientConfig, templateSource);

          TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes); 
          int maxPMapCountInBytes = TemplateCatalogConfig.maxPMapCountInBytes(catalog);             
          
          
          FASTClassLoader.deleteFiles();
   
          catalog.clientConfig();
		catalog.clientConfig();
		FASTDecoder readerDispatch = DispatchLoader.loadDispatchReaderDebug(catBytes, RingBuffers.buildNoFanRingBuffers(new RingBuffer(new RingBufferConfig((byte)15, (byte)7, catalog.ringByteConstants(), catalog.getFROM()))));
       //  FASTDecoder readerDispatch = new FASTReaderInterpreterDispatch(catBytes); 
         
         System.out.println("Using: "+readerDispatch.getClass().getSimpleName());
          
          final AtomicInteger msgs = new AtomicInteger();
          
                  
          int queuedBytes = 10;

          int iter = count;
          while (--iter >= 0) {
              
              InputStream instr = testDataInputStream(dataSource);
              PrimitiveReader reader = new PrimitiveReader(4096*1024, new FASTInputStream(instr), maxPMapCountInBytes);

              PrimitiveReader.fetch(reader);//Pre-load the file so we only count the parse time.
                            
              FASTReaderReactor reactor = new FASTReaderReactor(readerDispatch, reader);
                            
              msgs.set(0);                    
              
              
              double duration = single ?
                                singleThreadedExample(readerDispatch, msgs, reactor) :
                                multiThreadedExample(readerDispatch, msgs, reactor, reader);
              
                            
              if (shouldPrint(iter)) {
                  printSummary(msgs.get(), queuedBytes, duration, PrimitiveReader.totalRead(reader)); 
              }

              //reset the dictionary to run the test again.
              FASTDecoder.reset(catalog.dictionaryFactory(), readerDispatch);
              try {
                instr.close();
            } catch (IOException e) {
                
                e.printStackTrace();
                fail(e.getMessage());
            }

          }

      }

    private double singleThreadedExample(FASTDecoder readerDispatch, final AtomicInteger msgs, FASTReaderReactor reactor) {
        
        double start = System.nanoTime();
          
          /////////////////////////////////////
          //Example of single threaded usage
          /////////////////////////////////////
          RingBuffers ringBuffers = readerDispatch.ringBuffers;
          RingBuffer rb = RingBuffers.get(ringBuffers, 0);  

          boolean ok = true;
          int bufId;
          char[] temp = new char[64];
          while (ok) {

              switch (bufId = FASTReaderReactor.pump(reactor)) {
                  case -1://end of file
                      ok = false;
                      break;
                  case 0: //no room to read
                      break;
                  case 1: //read one fragment
                      
                      
                      
                      if (RingReader.tryReadFragment(rb)) {
                          
                          if (RingReader.isNewMessage(rb.ringWalker)) {
                        	  
                        	  if (RingReader.getMsgIdx(rb)<0) {
                        		  break;
                        	  }
                        	  
                              msgs.incrementAndGet();
                              
                              //processMessage(temp, rb); 
                              
                          } 
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
    
    private double multiThreadedExample(FASTDecoder readerDispatch, final AtomicInteger msgs, final FASTReaderReactor reactor, PrimitiveReader reader) {

    //    System.err.println("*************************************************************** multi test instance begin ");
        
        RingBuffer[] buffers = RingBuffers.buffers(readerDispatch.ringBuffers);
                
        int reactors = 1;
        final ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(reactors+buffers.length); 
        
        double start = System.nanoTime();
        
        final AtomicBoolean isAlive = reactor.start(executor, reader);

        int b = buffers.length;
        while (--b>=0) {
            final RingBuffer rb = buffers[b]; //Too many buffers!
            Runnable run = new Runnable() {
                char[] temp = new char[64];
                
                @Override
                public void run() {
                    int totalMessages = 0;
                    do {                        
                        //NOTE: the stats object shows that this is empty 75% of the time, eg needs more

                        if (RingReader.tryReadFragment(rb)) { 
                                assert(RingReader.isNewMessage(rb.ringWalker)) : "";
                                if (RingReader.getMsgIdx(rb)<0 ){
                                	break;
                                }
                                totalMessages++;
                                processMessage(temp, rb, reactor);  
                        } 
//                        else {
//                            //must wait on more to be written into the ring buffer before they can be read
//                            //the code is pushed a bit too hard so we have a lot of extra cpu cycles on the reader side to play with.
////                            int x = 4000;
////                            while (--x>=0) {
////                                Thread.yield();
////                            }
//                        }                        
                    } while (totalMessages<30000 || isAlive.get());
                    
                    //is alive is done writing but we need to empty out
                    while (RingReader.tryReadFragment(rb)) { 
                        if (RingReader.isNewMessage(rb.ringWalker)) {
                        	if (RingReader.getMsgIdx(rb)<0) {
                        		break;
                        	}
                        	
                            totalMessages++;
                        }
                    }
                    msgs.addAndGet(totalMessages);  
                }
                
            };
            executor.execute(run);
        }
    
        
        while (msgs.get()<3000 ||  isAlive.get()) {
        }
        
        // Only shut down after is alive is finished.
        executor.shutdown();
        
        try {
            executor.awaitTermination(1,TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        double duration = System.nanoTime() - start;
        
        
  //      System.err.println("Mean Latency:"+FASTRingBufferConsumer.responseTime(buffers[0].consumerData)+"ns");
        
//        System.err.println("finished one test  "+ buffers[0].consumerData.queueFill.toString()+" out of "+buffers[0].mask);
//        System.err.println("                   "+ buffers[0].consumerData.timeBetween.toString());
        
       // executor.shutdownNow();
        
  //      System.err.println("*************************************************************** multi test instance end ");
      
        return duration;
    }
    //TODO: C, need test for optional groups this is probably broken. 

    
    int IDX_TemplateId = 0;
    int IDX_Preamble = 1;
    
    int IDX1_AppVerId;
    int IDX1_MessageType;
    int IDX1_SenderCompID;
    int IDX1_MsgSeqNum;
    int IDX1_SendingTime;
    int IDX1_TradeDate;    
    int IDX1_NoMDEntries;
    
    int IDX1_MDUpdateAction;
    int IDX1_MDPriceLevel;    
    int IDX1_MDEntryType;
    int IDX1_OpenCloseSettleFlag;
    int IDX1_SecurityIDSource;
    int IDX1_SecurityID;
    int IDX1_RptSeq;
    int IDX1_MDEntryPx;    
    int IDX1_MDEntryTime;
    int IDX1_MDEntrySize;
    int IDX1_NumberOfOrders;
    int IDX1_TradingSessionID;
    int IDX1_NetChgPrevDay;
    int IDX1_TradeVolume;
    int IDX1_TradeCondition;
    int IDX1_TickDirection;
    int IDX1_QuoteCondition;
    int IDX1_AggressorSide;
    int IDX1_MatchEventIndicator;
    
    int IDX2_AppVerId;
    int IDX2_MessageType;
    int IDX2_SenderCompID;
    int IDX2_MsgSeqNum;
    
    boolean isInit;
    
    public void populateFieldIDs(FieldReferenceOffsetManager from, FASTReaderReactor reactor) {
        
        
        if (!isInit) {
            
            int fragStart;

            
            int[] msgStarts = from.messageStarts();
            
           	fragStart = msgStarts[0];
           	

            IDX1_AppVerId = FieldReferenceOffsetManager.lookupFieldLocator("ApplVerID", fragStart, from); 
            IDX1_MessageType = FieldReferenceOffsetManager.lookupFieldLocator("MessageType", fragStart, from);
            IDX1_SenderCompID = FieldReferenceOffsetManager.lookupFieldLocator("SenderCompID", fragStart, from);
            IDX1_MsgSeqNum = FieldReferenceOffsetManager.lookupFieldLocator("MsgSeqNum", fragStart, from);
            IDX1_SendingTime = FieldReferenceOffsetManager.lookupFieldLocator("SendingTime", fragStart, from);
            IDX1_TradeDate = FieldReferenceOffsetManager.lookupFieldLocator("TradeDate", fragStart, from);            
            IDX1_NoMDEntries = FieldReferenceOffsetManager.lookupFieldLocator("NoMDEntries", fragStart, from);
            
            IDX1_MDUpdateAction = FieldReferenceOffsetManager.lookupFieldLocator("MDUpdateAction", fragStart, from);
            IDX1_MDPriceLevel = FieldReferenceOffsetManager.lookupFieldLocator("MDPriceLevel", fragStart, from);            
            IDX1_MDEntryType = FieldReferenceOffsetManager.lookupFieldLocator("MDEntryType", fragStart, from);
            IDX1_OpenCloseSettleFlag = FieldReferenceOffsetManager.lookupFieldLocator("OpenCloseSettleFlag", fragStart, from);
            IDX1_SecurityIDSource = FieldReferenceOffsetManager.lookupFieldLocator("SecurityIDSource", fragStart, from);
            IDX1_SecurityID = FieldReferenceOffsetManager.lookupFieldLocator("SecurityID", fragStart, from);
            IDX1_RptSeq = FieldReferenceOffsetManager.lookupFieldLocator("RptSeq", fragStart, from);
            IDX1_MDEntryPx = FieldReferenceOffsetManager.lookupFieldLocator("MDEntryPx", fragStart, from);            
            IDX1_MDEntryTime = FieldReferenceOffsetManager.lookupFieldLocator("MDEntryTime", fragStart, from);
            IDX1_MDEntrySize = FieldReferenceOffsetManager.lookupFieldLocator("MDEntrySize", fragStart, from);
            IDX1_NumberOfOrders = FieldReferenceOffsetManager.lookupFieldLocator("NumberOfOrders", fragStart, from);
            IDX1_TradingSessionID = FieldReferenceOffsetManager.lookupFieldLocator("TradingSessionID", fragStart, from);
            IDX1_NetChgPrevDay = FieldReferenceOffsetManager.lookupFieldLocator("NetChgPrevDay", fragStart, from);
            IDX1_TradeVolume = FieldReferenceOffsetManager.lookupFieldLocator("TradeVolume", fragStart, from);
            IDX1_TradeCondition = FieldReferenceOffsetManager.lookupFieldLocator("TradeCondition", fragStart, from);
            IDX1_TickDirection = FieldReferenceOffsetManager.lookupFieldLocator("TickDirection", fragStart, from);
            IDX1_QuoteCondition = FieldReferenceOffsetManager.lookupFieldLocator("QuoteCondition", fragStart, from);
            IDX1_AggressorSide = FieldReferenceOffsetManager.lookupFieldLocator("AggressorSide", fragStart, from);
            IDX1_MatchEventIndicator = FieldReferenceOffsetManager.lookupFieldLocator("MatchEventIndicator", fragStart, from);
            
            
            //TODO: B, this is the beginning of a unit test.
            validate("ApplVerID", 2, IDX1_AppVerId);
            validate("MessageType", 4, IDX1_MessageType);
            validate("SenderCompID", 6, IDX1_SenderCompID);
            validate("MsgSeqNum", 8, IDX1_MsgSeqNum);
            validate("SendingTime", 9, IDX1_SendingTime);
            validate("TradeDate", 10, IDX1_TradeDate);            
            validate("NoMDEntries", 11, IDX1_NoMDEntries);
            
            validate("MDUpdateAction", 0, IDX1_MDUpdateAction);
            validate("MDPriceLevel", 1, IDX1_MDPriceLevel);
            validate("MDEntryType", 2, IDX1_MDEntryType);
            validate("OpenCloseSettleFlag", 4, IDX1_OpenCloseSettleFlag);
            validate("SecurityIDSource", 5, IDX1_SecurityIDSource);
            validate("SecurityID", 6, IDX1_SecurityID);
            validate("RptSeq", 7, IDX1_RptSeq);
            validate("MDEntryPx", 8, IDX1_MDEntryPx);            
            validate("MDEntryTime", 11, IDX1_MDEntryTime);
            validate("MDEntrySize", 12, IDX1_MDEntrySize);
            validate("NumberOfOrders", 13, IDX1_NumberOfOrders);
            validate("TradingSessionID", 14, IDX1_TradingSessionID);
            validate("NetChgPrevDay", 16, IDX1_NetChgPrevDay);
            validate("TradeVolume", 19, IDX1_TradeVolume);
            validate("TradeCondition", 20, IDX1_TradeCondition);
            validate("TickDirection", 22, IDX1_TickDirection);
            validate("QuoteCondition", 24, IDX1_QuoteCondition);
            validate("AggressorSide", 26, IDX1_AggressorSide);
            validate("MatchEventIndicator", 27, IDX1_MatchEventIndicator);
            
            fragStart = msgStarts[1];
            
            IDX2_AppVerId = FieldReferenceOffsetManager.lookupFieldLocator("ApplVerID", fragStart, from); 
            IDX2_MessageType = FieldReferenceOffsetManager.lookupFieldLocator("MessageType", fragStart, from);
            IDX2_SenderCompID = FieldReferenceOffsetManager.lookupFieldLocator("SenderCompID", fragStart, from);
            IDX2_MsgSeqNum = FieldReferenceOffsetManager.lookupFieldLocator("MsgSeqNum", fragStart, from);
            
            validate("ApplVerID", 2, IDX2_AppVerId);
            validate("MessageType", 4, IDX2_MessageType);
            validate("SenderCompID", 6, IDX2_SenderCompID);
            validate("MsgSeqNum", 8, IDX2_MsgSeqNum);
            
            templateId = 99;
            fragStart = msgStarts[3];
            
            
            
            isInit = true;
        }
        
    }
    
    private void validate(String message, int expectedOffset, int id) {
        if (expectedOffset!=(RingReader.OFF_MASK&id)) {
            System.err.println("expected: "+expectedOffset+" but found "+id+" for "+message);
        }
    }
    
    
    private void processMessage(char[] temp, RingBuffer rb, FASTReaderReactor reactor) {
       
        populateFieldIDs(rb.ringWalker.from, reactor); 


        templateId = readInt(rb, IDX_TemplateId);
        preamble = readInt(rb, IDX_Preamble);

        switch (RingReader.getMsgIdx(rb.ringWalker)) {
            case 1:
                
                if (!eqASCII(rb, IDX1_AppVerId, "1.0")) {
                    throw new UnsupportedOperationException("Does not support version "+readASCII(rb, IDX1_AppVerId, new StringBuilder()));
                }
              
                int len;
                len = readDataLength(rb, IDX1_MessageType);
                readASCII(rb, IDX1_MessageType, temp, 0);
                // System.err.println("MessageType: "+new String(temp,0,len));
    
                len = readDataLength(rb, IDX1_SenderCompID);
                readASCII(rb, IDX1_SenderCompID, temp, 0);
                // System.err.println("SenderCompID: "+new String(temp,0,len));
    
                int msgSeqNum = readInt(rb, IDX1_MsgSeqNum);
                int sendingTime = readInt(rb, IDX1_SendingTime);
                int tradeDate = readInt(rb, IDX1_TradeDate);
                int seqCount = readInt(rb, IDX1_NoMDEntries);
                // System.err.println(sendingTime+" "+tradeDate+" "+seqCount);
                while (--seqCount >= 0) {
                    while (!RingReader.tryReadFragment(rb)) { // keep calling if we
                                                           // have no data?
                    };
                    
                    int mDUpdateAction = readInt(rb, IDX1_MDUpdateAction);
                    int mDPriceLevel = readInt(rb, IDX1_MDPriceLevel);
    
                    readASCII(rb, IDX1_MDEntryType, temp, 0);
    
                    int openCloseSettleFlag = readInt(rb, IDX1_OpenCloseSettleFlag);
                    int securityIDSource = readInt(rb, IDX1_SecurityIDSource);
                    int securityID = readInt(rb, IDX1_SecurityID);
                    int rptSeq = readInt(rb, IDX1_RptSeq);
                    // MDEntryPx
                    int mDEntryPxExpo = readDecimalExponent(rb, IDX1_MDEntryPx);
                    long mDEntrypxMant = readDecimalExponent(rb, IDX1_MDEntryPx);
                    int mDEntryTime = readInt(rb, IDX1_MDEntryTime);
                    int mDEntrySize = readInt(rb, IDX1_MDEntrySize);
                    int numberOfOrders = readInt(rb, IDX1_NumberOfOrders);
                                                            
                    len = readDataLength(rb, IDX1_TradingSessionID);
                    readASCII(rb, IDX1_TradingSessionID, temp, 0); 
                    // System.err.println("TradingSessionID: "+new String(temp,0,len));
    
                    int netChgPrevDayExpo = readDecimalExponent(rb, IDX1_NetChgPrevDay);
                    long netChgPrevDayMant = readDecimalMantissa(rb, IDX1_NetChgPrevDay);
                    
                    int tradeVolume = readInt(rb, IDX1_TradeVolume);
                    
                    len = readDataLength(rb, IDX1_TradeCondition);
                    if (len>0) {
                    readASCII(rb, IDX1_TradeCondition, temp, 0); 
                    // System.err.println("TradeCondition: "+new String(temp,0,len));
                    }
                    
                    len = readDataLength(rb, IDX1_TickDirection);
                    if (len>0) {
                    readASCII(rb, IDX1_TickDirection, temp, 0); 
                    // System.err.println("TickDirection: "+new String(temp,0,len));
                    }
                    
                    len = readDataLength(rb, IDX1_QuoteCondition);
                    if (len>0) {
                    readASCII(rb, IDX1_QuoteCondition, temp, 0); 
                    // System.err.println("QuoteCondition: "+new String(temp,0,len));
                    }
                    int aggressorSide = readInt(rb, IDX1_AggressorSide);
                  
                    len = readDataLength(rb, IDX1_MatchEventIndicator);
                    if (len>0) {
                    readASCII(rb, IDX1_MatchEventIndicator, temp, 0); 
                    // System.err.println("MatchEventIndicator: "+new String(temp,0,len));
                    }
                    
                }
    
                break;
            case 2:
    
               len = readDataLength(rb, IDX2_AppVerId);
               readASCII(rb, IDX2_AppVerId, temp, 0);
               //System.err.println("ApplVerID: "+new String(temp,0,len));
               
               len = readDataLength(rb, IDX2_MessageType);
               readASCII(rb, IDX2_MessageType, temp, 0);
               //System.err.println("MessageType: "+new String(temp,0,len));
               
               len = readDataLength(rb, IDX2_SenderCompID);
               readASCII(rb, IDX2_SenderCompID, temp, 0);
             //  System.err.println("SenderCompID: "+new String(temp,0,len));
               
               int msgSeqNum2 = readInt(rb, IDX2_MsgSeqNum);
               int sendingTime2 = readInt(rb, 9);
               
               len = readDataLength(rb, 10);
               if (len>0) {
                   readASCII(rb, 10, temp, 0);
                   //System.err.println("QuoteReqID: "+new String(temp,0,len));
               }
               
               int seqCount2 = readInt(rb, 12);
               
               while (--seqCount2 >= 0) {
                   while (!RingReader.tryReadFragment(rb)) { // keep calling if we
                                                          // have no data?
                      
                       len = readDataLength(rb, 0);
                       readASCII(rb, 0, temp, 0); 
                       
                       long orderQty = readLong(rb, 2);
                       int side = readInt(rb, 4);
                       long transactTime = readLong(rb, 5);
                       int quoteType = readInt(rb, 7);
                       int securityID = readInt(rb, 8);
                       int securityIDSource = readInt(rb, 9);
                       
                   };
                                                        
               }
               
         
            
                break;
            case 99:
    
                len = readDataLength(rb, 2);
                readASCII(rb, 2, temp, 0);
                // rb.tailPos.lazySet(rb.workingTailPos.value);
                // System.err.println("MessageType: "+new String(temp,0,len));
    
                break;
            default:
                System.err.println("Did not expect " + RingReader.getMsgIdx(rb.ringWalker));
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
        
        InputStream resourceInput = ThreadingTest.class.getResourceAsStream(resource);
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
