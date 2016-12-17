package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeaderKey;
import com.ociweb.pronghorn.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.Pipe.PaddedLong;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTP1xRouterStage<T extends Enum<T> & HTTPContentType,
                             H extends Enum<H> & HTTPHeaderKey,
                             R extends Enum<R> & HTTPRevision,
                             V extends Enum<V> & HTTPVerb> extends PronghornStage {

  //TODO: double check that this all works iwth ipv6.
    
    private static Logger logger = LoggerFactory.getLogger(HTTP1xRouterStage.class);
    
    private TrieParser verbMap;
    private TrieParser urlMap;
    private TrieParser headerMap;
    private TrieParser revisionMap;
        
    private TrieParserReader trieReader;
    
    private final Pipe<NetPayloadSchema>[] inputs;
    private       long[]                      inputChannels;
    private       int[]                       inputBlobPos;
    private       int[]                       inputLengths;
    private       boolean[]                   isOpen;
        
    private final Pipe<ReleaseSchema> releasePipe;
    
    private final Pipe<HTTPRequestSchema>[] outputs;

    private final int[]                     messageIds;
    private DataOutputBlobWriter<HTTPRequestSchema>[] blobWriter;
    private long[] requestHeaderMask;
    private byte[][] headerOffsets;
    private int[][] headerBlankBases;
    private long[] inputSlabPos;
    
    private static int MAX_HEADER = 1<<15;
    
    private final CharSequence[] paths;
    
    private int sequenceNo = 0;
    
    //TODO: need error if two strings are added to the same place?? or not
    //TODO: all the parts to the same message will share the same sequence no

    private final HTTPSpecification<T,R,V,H> httpSpec;
        
    private int headerIdUpgrade    = HTTPHeaderKeyDefaults.UPGRADE.ordinal();
    private int headerIdConnection = HTTPHeaderKeyDefaults.CONNECTION.ordinal();
    private int headerIdContentLength = HTTPHeaderKeyDefaults.CONTENT_LENGTH.ordinal();
    
    private final int END_OF_HEADER_ID;
    private final int UNKNOWN_HEADER_ID;
    
    
    private int totalShortestRequest;
    private int shutdownCount;
    
    private int idx;
    
    //read all messages and they must have the same channelID
    //total all into one master DataInputReader
       
    
    public static HTTP1xRouterStage newInstance(GraphManager gm, Pipe<NetPayloadSchema>[] input, Pipe<HTTPRequestSchema>[] outputs, Pipe<ReleaseSchema> ackStop,
                                              CharSequence[] paths, long[] headers, int[] messageIds) {
        
       return new HTTP1xRouterStage<HTTPContentTypeDefaults ,HTTPHeaderKeyDefaults, HTTPRevisionDefaults, HTTPVerbDefaults>(gm,input,outputs, ackStop, paths, headers, messageIds,
                                   HTTPSpecification.defaultSpec()  ); 
    }

	public HTTP1xRouterStage(GraphManager gm, Pipe<NetPayloadSchema>[] input, Pipe<HTTPRequestSchema>[] outputs, Pipe<ReleaseSchema> ackStop,
                           CharSequence[] paths, long[] headers, int[] messageIds, 
                           HTTPSpecification<T,R,V,H> httpSpec) {
        super(gm,input,join(outputs,ackStop));
        this.inputs = input;
        this.releasePipe = ackStop;        
        this.outputs = outputs;
        this.messageIds = messageIds;
        this.requestHeaderMask = headers;
        this.shutdownCount = inputs.length;
        
        this.httpSpec = httpSpec;
        
        this.paths = paths;
        
        END_OF_HEADER_ID  = httpSpec.headerCount+2;//for the empty header found at the bottom of the header
        UNKNOWN_HEADER_ID = httpSpec.headerCount+1;
        		
        supportsBatchedPublish = false;
        supportsBatchedRelease = false;
    }
    
    
    @Override
    public void startup() {
        
    	idx = inputs.length;
    	
        //keeps the channel used by this pipe to ensure these are never mixed.
        //also used to release the subscription? TODO: this may be a race condition, not sure ...
        inputChannels = new long[inputs.length];
        Arrays.fill(inputChannels, -1);
        inputBlobPos = new int[inputs.length];
        inputLengths = new int[inputs.length];
        isOpen = new boolean[inputs.length];
        Arrays.fill(isOpen, true);
        
        inputSlabPos = new long[inputs.length];
        
        final int sizeOfVarField = 2;
        
        int h = requestHeaderMask.length;
        headerOffsets = new byte[h][];
        headerBlankBases = new int[h][];
        
        while (--h>=0) { 
            byte[] offsets = new byte[httpSpec.headerCount+1];
            byte runningOffset = 0;
            
            long mask = requestHeaderMask[h];
            int fieldCount = 0;
            for(int ordinalValue = 0; ordinalValue<=httpSpec.headerCount; ordinalValue++) {
                //only set fields for the bits which are on, and do in this order.
                if (0!=(1&(mask>>ordinalValue))) {
                    offsets[ordinalValue] = runningOffset;
                    runningOffset += sizeOfVarField;
                    fieldCount++;
                }
            }
            
            headerOffsets[h] = offsets;            
            headerBlankBases[h] = buildEmptyBlockOfVarDatas(fieldCount);
            
        }

        ///
        
        urlMap = new TrieParser(1024,true);  //TODO: build size needed for these URLs dynamically 
        headerMap = new TrieParser(2048,false);//do not skip deep checks, we do not know which new headers may appear.
        
        verbMap = new TrieParser(256,false);//does deep check
        revisionMap = new TrieParser(256,true); //avoid deep check
        
        trieReader = new TrieParserReader(16);//max fields we support capturing.
        
        int w = outputs.length;
        blobWriter = new DataOutputBlobWriter[w];
        while (--w>=0) {
            blobWriter[w] = new DataOutputBlobWriter<HTTPRequestSchema>(outputs[w]);
        }
        
        int x; 
        
        totalShortestRequest = 0;//count bytes for the shortest known request, this opmization helps prevent parse attempts when its clear that there is not enough data.
        int localShortest;
        
        headerMap.setUTF8Value("\n", END_OF_HEADER_ID); //Detecting this first but not right!! we did not close the revision??
        headerMap.setUTF8Value("\r\n", END_OF_HEADER_ID);
        
        //TODO: this addition is slowing down the performance, must investigate
        headerMap.setUTF8Value("%b: %b\n", UNKNOWN_HEADER_ID); //TODO: bug in trie if we attemp to set this first...
        headerMap.setUTF8Value("%b: %b\r\n", UNKNOWN_HEADER_ID);        

        //Load the supported header keys
        H[] shr =  httpSpec.supportedHTTPHeaders.getEnumConstants();
        x = shr.length;
        while (--x >= 0) {
            //must have tail because the first char of the tail is required for the stop byte
            headerMap.setUTF8Value(shr[x].getKey(), "\n",shr[x].ordinal());
            headerMap.setUTF8Value(shr[x].getKey(), "\r\n",shr[x].ordinal());
        }     

        
        //a request may have NO header with just the end marker so add one
        totalShortestRequest+=1;
                
        //Load the supported HTTP revisions
        R[] revs = httpSpec.supportedHTTPRevisions.getEnumConstants();
        x = revs.length;
        localShortest = Integer.MAX_VALUE;                
        while (--x >= 0) {
            int b = revisionMap.setUTF8Value(revs[x].getKey(), "\n", revs[x].ordinal());
            if (b<localShortest) {
                localShortest = b;
            }            
            revisionMap.setUTF8Value(revs[x].getKey(), "\r\n", revs[x].ordinal());
        }
        totalShortestRequest += localShortest; //add shortest revision
                  
        //Load the supported HTTP verbs
        V[] verbs = httpSpec.supportedHTTPVerbs.getEnumConstants();
        x = verbs.length;
        localShortest = Integer.MAX_VALUE;
        while (--x >= 0) {
            int b = verbMap.setUTF8Value(verbs[x].getKey()," ", verbs[x].ordinal());
            if (b<localShortest) {
                localShortest = b;
            }            
        }
        totalShortestRequest += localShortest; //add shortest verb
        
        //load all the routes
        x = paths.length;
        localShortest = Integer.MAX_VALUE;
        while (--x>=0) {

            int b;
            int value = x;
            if (' '==paths[x].charAt(paths[x].length()-1)) {
                b=urlMap.setUTF8Value(paths[x], value);
            } else {
                b=urlMap.setUTF8Value(paths[x], " ",value);
            }
            if (b<localShortest) {
                localShortest = b;
            } 
        }
        totalShortestRequest += localShortest; 
        
        if (totalShortestRequest<=0) {
            totalShortestRequest = 1;
        }
        
        
    }
    

    @Override
    public void shutdown() {
        //TODO: send EOF 
    }
   

    
//a request example
//
//  GET /hello/x?x=3 HTTP/1.1
//  Host: 127.0.0.1:8081
//  Connection: keep-alive
//  Cache-Control: max-age=0
//  Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
//  Upgrade-Insecure-Requests: 1
//  User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/47.0.2526.106 Chrome/47.0.2526.106 Safari/537.36
//  DNT: 1
//  Accept-Encoding: gzip, deflate, sdch
//  Accept-Language: en-US,en;q=0.8

    
    @Override
    public void run() {
    	int didWork;
   	
    	do { 
    		didWork = 0;
	   
    		int m = 100;//max iterations before taking a break
	        while (--idx>=0 && --m>=0) {
	            int result = singlePipe(idx);
	            
	            if (result<0) {
	            	if (--shutdownCount==0) {
	            		requestShutdown();
	            		return;
	            	}
	            	
	            } else {
	            	didWork+=result;
	            }
	            
	            
	        }
	        if (idx<=0) {
	        	idx = inputs.length;
	        }
	        
    	} while (didWork!=0);    

    }

    
    //return 0, 1 work, -1 shutdown.
    public int singlePipe(int idx) {
        Pipe<NetPayloadSchema> selectedInput = inputs[idx];     

        if (isOpen[idx]) {
        	
            int messageIdx = accumulateRunningBytes(idx, selectedInput);
            if (messageIdx < 0) {
                isOpen[idx] = false;
                
                return -1;
                
            }
            //if (Integer.MAX_VALUE==messageIdx) { //not sure we should not optimize this return...
            //	return 0;//still testing
           // }
        }
        
        long channel   = inputChannels[idx];
        if (channel >= 0) {
        	
        	//logger.info("setup parse for channel {}",channel);
        	
            assert(inputLengths[idx]>=0) : "length is "+inputLengths[idx]; 
            TrieParserReader.parseSetup(trieReader, Pipe.blob(selectedInput), inputBlobPos[idx], inputLengths[idx], Pipe.blobMask(selectedInput));
         
            final long channel1 = channel; 
            return consumeAvail(idx, selectedInput, channel1, inputBlobPos[idx], inputLengths[idx]);            
        }
        
//        if (!isOpen[idx] && 0==inputLengths[idx]) {
//            Pipe.publishWorkingTailPosition(selectedInput, Pipe.headPosition(selectedInput));//wipe out pipe.       
//
//            return 1;
//        }        
        return 0;
    }


    private int consumeAvail(final int idx, Pipe<NetPayloadSchema> selectedInput, final long channel, int p, int l) {
        
    	    int consumed = 0;
            final long toParseLength = TrieParserReader.parseHasContentLength(trieReader);
            assert(toParseLength>=0) : "length is "+toParseLength+" and input was "+l;
            
            //TODO: check out client code and note how we do not attempt parse if no NEW data has been provided.
            if (toParseLength>=totalShortestRequest && route(trieReader, channel, idx, selectedInput)) {
                consumed = (int)(toParseLength - TrieParserReader.parseHasContentLength(trieReader));           
                Pipe.releasePendingAsReadLock(selectedInput, consumed);
                p = Pipe.blobMask(selectedInput) & (p + consumed);
                l -= consumed;                
                assert(l == trieReader.sourceLen);
                
                if (l<=0) {
                	inputChannels[idx] = -1;
                }
                
            } else {
                assert(l>=0) : "length is "+l;
            }

	        inputBlobPos[idx]=p;
	        inputLengths[idx]=l;
	        
	        return consumed>0?1:0;
    }


private boolean route(TrieParserReader trieReader, long channel, int idx, Pipe<NetPayloadSchema> selectedInput) {    
    
//	boolean showHeader = true;
//    if (showHeader) {
//    	System.out.println("///////////////// HEADER ///////////////////");
//    	TrieParserReader.debugAsUTF8(trieReader, System.out, trieReader.sourceLen, false); //shows that we did not get all the datas	
//    	System.out.println("\n///////////////////////////////////////////");
//    }

	//TODO: URGENT this parser must allow for letting go of what has already been parsed,
	
	
    final int verbId = (int)TrieParserReader.parseNext(trieReader, verbMap);     //  GET /hello/x?x=3 HTTP/1.1     
    if (verbId<0) {
    //	return false;//waiting for rest of data
    	
     //   if (TrieParserReader.parseHasContentLength(trieReader)<=5) { //TODO: how long until we timeout and abandon this partial data??
            //not error, must wait for more content and try again.
 //       	logger.info("waiting for {} verb",channel);
            
        	//TODO: is this rolling back the head as needed???
        	
            return false;
       // } else {
        	
        //	logger.info("is this an ERROR ZZZZZZZZZZZ  {}",channel);
            //TODO: send error
            
       // 	return false;
       // }
        
    }
    
  //  TrieParserReader.debugAsUTF8(trieReader, System.out,3000, false);
    final int routeId = (int)TrieParserReader.parseNext(trieReader, urlMap);     //  GET /hello/x?x=3 HTTP/1.1 
    if (routeId<0) {
//    	logger.info("A waiting for {} route",channel);
    	
    	//not error, must wait for more content and try again.
    	return false; 
    }
 
    Pipe<HTTPRequestSchema> staticRequestPipe = outputs[routeId];
    //System.out.println("static target "+routeId+" at head "+Pipe.headPosition(staticRequestPipe)); //TODO: nothing is written since we do not get enought data.

    Pipe.markHead(staticRequestPipe);//holds in case we need to abandon our writes
    
    if (Pipe.hasRoomForWrite(staticRequestPipe) ) {
        
        final int size =  Pipe.addMsgIdx(staticRequestPipe, messageIds[routeId]);        // Write 1   1                         
        Pipe.addLongValue(channel, staticRequestPipe); // Channel                        // Write 2   3        
        
       // System.out.println("wrote ever increasing squence from router of "+sequenceNo);//TODO: should this be per connection instead!!
        
        Pipe.addIntValue(sequenceNo++, staticRequestPipe); //sequence                    // Write 1   4
        Pipe.addIntValue(verbId, staticRequestPipe);   // Verb                           // Write 1   5
        
        writeURLParamsToField(trieReader, blobWriter[routeId]);                          //write 2   7
       
        int sp = trieReader.sourcePos;
        int sl = trieReader.sourceLen;
        
        int httpRevisionId = (int)TrieParserReader.parseNext(trieReader, revisionMap);  //  GET /hello/x?x=3 HTTP/1.1 
        
        if (httpRevisionId<0) { 
  //          if (TrieParserReader.parseHasContentLength(trieReader)<=5) {
                //ROLL BACK THE WRITE
                Pipe.resetHead(staticRequestPipe);
                //not error, must wait for more content and try again.
  //              logger.info("B waiting for {} {} {} room for write {}",channel, sp, sl, Pipe.hasRoomForWrite(staticRequestPipe));
                
                return false;            
//            } else {
//            	Pipe.resetHead(staticRequestPipe); 
//            	logger.info("is this an ERROR XXXXXXXXXXXXXXX  {}",channel);
//                
//                
//                //TODO: send error
//                return false;
//            }  

        }
        
        Pipe.addIntValue(httpRevisionId, staticRequestPipe); // Revision Id          // Write 1   8

        int requestContext = parseHeaderFields(routeId, staticRequestPipe, httpRevisionId, false);  // Write 2   10 //if header is presen
        if (ServerCoordinator.INCOMPLETE_RESPONSE_MASK == requestContext) {   
            //try again later, not complete.
            Pipe.resetHead(staticRequestPipe);
            return false;
        }        
        Pipe.addIntValue(requestContext, staticRequestPipe); // request context     // Write 1   11
        
        int consumed = Pipe.publishWrites(staticRequestPipe);                        // Write 1   12
        assert(consumed>=0);        
        Pipe.confirmLowLevelWrite(staticRequestPipe, size);     

        if (trieReader.sourceLen==0 && 
        	Pipe.contentRemaining(selectedInput)==0) { //added second rule to minimize release messages
        	
        	assert(inputSlabPos[idx]>=0);
        	//logger.info("send ack for {}",channel);
        	
        	if (Pipe.hasRoomForWrite(releasePipe)) {
        		int s = Pipe.addMsgIdx(releasePipe, ReleaseSchema.MSG_RELEASE_100);
        		Pipe.addLongValue(channel,releasePipe);
        		Pipe.addLongValue(inputSlabPos[idx],releasePipe);
        		Pipe.confirmLowLevelWrite(releasePipe, s);
        		Pipe.publishWrites(releasePipe);
        		this.inputSlabPos[idx]=-1;
        	} else {
				logger.error("BBBBBBBBBBBBBBBBB server no room for ack of {} {}",channel,releasePipe);
			}
        } 
        
    } else {
        //no room try again later
        return false;
    }
    
   return true;
}


private void writeURLParamsToField(TrieParserReader trieReader, DataOutputBlobWriter<HTTPRequestSchema> writer) {

    DataOutputBlobWriter.openField(writer);
    try {
        TrieParserReader.writeCapturedValuesToDataOutput(trieReader,writer);
    } catch (IOException e) {        
        //ignore, will not throw writing to field not stream
    }
    DataOutputBlobWriter.closeLowLevelField(writer);
}



private int accumulateRunningBytes(int idx, Pipe<NetPayloadSchema> selectedInput) {
    
    int messageIdx = Integer.MAX_VALUE;

//    logger.info("accumulate data rules {} && ({} || {} || {})",
//    		     Pipe.hasContentToRead(selectedInput), 
//    		     hasNoActiveChannel(idx), 
//    		     hasReachedEndOfStream(selectedInput), 
//    		     hasContinuedData(idx, selectedInput) );
    
    while (Pipe.hasContentToRead(selectedInput) && //NOTE has content to read looks at slab position between last read and new head.
            
            (hasNoActiveChannel(idx) ||      //if we do not have an active channel
             hasReachedEndOfStream(selectedInput) || //if we have reached the end of the stream
              hasContinuedData(idx, selectedInput) 
           )            
          ) {
    
        
        messageIdx = Pipe.takeMsgIdx(selectedInput);
        if (NetPayloadSchema.MSG_PLAIN_210 == messageIdx) {
            long channel   = Pipe.takeLong(selectedInput);
            this.inputSlabPos[idx] = Pipe.takeLong(selectedInput);
          
            int meta       = Pipe.takeRingByteMetaData(selectedInput);
            int length     = Pipe.takeRingByteLen(selectedInput);
            int pos        = Pipe.bytePosition(meta, selectedInput, length);                                            
            
            assert(Pipe.byteBackingArray(meta, selectedInput) == Pipe.blob(selectedInput));            
            assert(length>0) : "value:"+length;
            assert(Pipe.validateVarLength(selectedInput, length));
            

            if (-1 == inputChannels[idx]) {
                //assign
                inputChannels[idx]  = channel;
                inputLengths[idx]   = length;
                inputBlobPos[idx]   = pos;
     
                assert(inputLengths[idx]<selectedInput.sizeOfBlobRing);
            } else {
                //confirm match
                assert(inputChannels[idx] == channel) : "Internal error, mixed channels";
                   
                //grow position
                inputLengths[idx] += length; 
                
                assert(inputLengths[idx]<Pipe.blobMask(selectedInput)) : "When we roll up is must always be smaller than ring";
                
                //may only read up to safe point where head is
                assert(validatePipePositions(selectedInput, inputBlobPos[idx], inputLengths[idx]));
                                
            }
            
            //if we do not move this forward we will keep reading the same spot up to the new head
            Pipe.confirmLowLevelRead(selectedInput, Pipe.sizeOf(selectedInput, messageIdx)); 
            
            Pipe.readNextWithoutReleasingReadLock(selectedInput); 
            
            if (-1 == inputSlabPos[idx]) {
            	inputSlabPos[idx] = Pipe.getWorkingTailPosition(selectedInput); //working and was tested since this is low level with unrleased block.
            }
            assert(inputSlabPos[idx]!=-1);
        } else {
            assert(-1 == messageIdx) : "messageIdx:"+messageIdx;
            if (-1 != messageIdx) {
            	throw new UnsupportedOperationException("bad id "+messageIdx);
            }
            
            Pipe.confirmLowLevelRead(selectedInput, Pipe.EOF_SIZE);
            Pipe.readNextWithoutReleasingReadLock(selectedInput);
            return messageIdx;//do not loop again just exit now
        }        
    }
 
    return messageIdx;
}


private boolean validatePipePositions(Pipe<NetPayloadSchema> selectedInput, int p, int l) {
    int head = Pipe.getBlobRingHeadPosition(selectedInput);
    int mhead = Pipe.blobMask(selectedInput) & head;
    int mstart = Pipe.blobMask(selectedInput) & p;
    int stop = mstart+l;
    int mstop = Pipe.blobMask(selectedInput) & stop;
         
    if (stop >= selectedInput.sizeOfBlobRing) {
       assert(mstop<=mhead) : "Pipe:"+selectedInput;
       assert(mhead<=mstart) : "Pipe:"+selectedInput;
    } else {
       assert(stop<=mhead || mhead<=mstart) : "mhead :"+mhead+" between "+mstart+" and "+stop+"\nPipe:"+selectedInput;                 
    }
    return true;
}


private boolean hasContinuedData(int idx, Pipe<NetPayloadSchema> selectedInput) {
    return Pipe.peekLong(selectedInput, 1)==inputChannels[idx];
}


private boolean hasReachedEndOfStream(Pipe<NetPayloadSchema> selectedInput) {
    return Pipe.peekInt(selectedInput)<0;
}


private boolean hasNoActiveChannel(int idx) {
    return -1 == inputChannels[idx];
}
    
    /*
     * Message format
     *  long channel
     *  int  verb
     *  var  capturedFields and or capturedURL
     *  int  revision
     *  var  requested headers in enum order ??
     * 
     */

    private int parseHeaderFields(final int routeId, Pipe<HTTPRequestSchema> staticRequestPipe, int revisionId, boolean debugMode) {
        long headerMask = requestHeaderMask[routeId];
        
        int requestContext = keepAliveOrNotContext(revisionId);
                      
        
        //this call moves the workspace head to after this new block but returns the postion at its front.
        long basePos = addZeroLengthVarFields(staticRequestPipe, headerBlankBases[routeId]);
        
        byte[] offsets = headerOffsets[routeId];
        
        
        //NOTE:
        //   some commands can redirect, eg upgrade
        //   we are already writing to the output pipe so we can not make a change.
        //   upgrade is a command that goes to the module so it can reply with ok
        //      the reply will set the pipe so reader will redirect BEFORE route.
              
        //continue to parse while there is data and while we still need to find some remaining headers.
        //NOTE this stops as soon as all the desired headers are found 

        int iteration = 0;
        boolean isDone = false;
        int remainingLen;
        while ((remainingLen=TrieParserReader.parseHasContentLength(trieReader))>0){
        
        	boolean watch = false;
        	int alen=trieReader.sourceLen;
        	int apos=trieReader.sourcePos;
        	if (12==trieReader.sourceLen) {
        		watch = true;        		
        	}
        	int headerId = (int)TrieParserReader.parseNext(trieReader, headerMap);
        	       	
        	
            if (END_OF_HEADER_ID == headerId) {
                if (iteration==0) {
                    throw new RuntimeException("should not have found end of header");
                } else {
                     isDone = true;
                     assignMissingHeadersNull(staticRequestPipe, headerMask, basePos, offsets);
                }             
               // logger.info("end of request found");
                return requestContext;                
            }
            
            if (-1 == headerId) {            	
            	if (remainingLen>MAX_HEADER) {
            		throw new RuntimeException("client has sent bad data, this connection should be closed so we can move on");//TODO: remove this exeption and do what it says.
            	}
                //nothing valid was found so this is incomplete.
                return ServerCoordinator.INCOMPLETE_RESPONSE_MASK; 
            }

            if (headerIdUpgrade == headerId) {
                //list of protocols to upgrade to 
                
                //IF TEXT  CONTAINS
                //h2c       upgrade to that
                //websocket
                
                //if no match is found must do deeper parse
                
                //clear internal mask bit
            	
            	logger.warn("upgrade reqeust deteced but not yet implemented.");
            } else if (headerIdContentLength == headerId) {
            	
            	logger.warn("post or payload not yet implemented, content length detected");
            	
              
            } else if (headerIdConnection == headerId) {
            	if (watch) {

            		trieReader.sourceLen = alen;
            		trieReader.sourcePos = apos;
            		int temp = (int)TrieParserReader.parseNext(trieReader, headerMap);
               	 
            		System.err.println("length to parse from "+trieReader.sourceLen+"  "+headerId+"=="+temp);
            		
            	}
            	
                requestContext = applyKeepAliveOrCloseToContext(requestContext);                
            }
            

            
            int maskId = 1<<headerId;
            
            if (0 == (maskId&headerMask) ) {
                //skip this data since the app module can not make use of it
                //this is the normal most frequent case                    
            } else {
            	
            	if (true) { 
            		throw new UnsupportedOperationException("only simple rest calls can be made, non should require the header values.");
            	}
            	
                //clear this bit to mark it as seen
                headerMask = headerMask^maskId;

                //copy the caputred bytes to ...
                //TODO: must write these into a var length field with leading id for each 
                TrieParserReader.writeCapturedValuesToPipe(trieReader, staticRequestPipe, basePos+offsets[headerId]);
           
            }      
         
            iteration++;
        }
        if (!isDone) {
            return ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
        }
        return requestContext;
    }


	private int applyKeepAliveOrCloseToContext(int requestContext) {
		//LOOK FOR ONE OF THE FOLLOWING, GRAB SECOND CHAR
		//close            SET CLOSE FOR ALL
		//keep-alive       CLEAR CLOSE FOR ALL
		//Upgrade          SET PIPE UPGRADE BIT
		//Upgrade, HTTP2-Settings
		
		//if no match is found must do deeper parse
		
		//clear internal mask bit, may need other http2 settings.
		
		int len = TrieParserReader.capturedFieldByte(trieReader, 0, 1);
		
		//logger.warn("Connection: {}",TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, new StringBuilder()) ); 
		
		switch(len) {
		    case 'l': //close
		        requestContext |= ServerCoordinator.CLOSE_CONNECTION_MASK;                        
		        break;
		    case 'e': //keep-alive
		        requestContext &= (~ServerCoordinator.CLOSE_CONNECTION_MASK);                        
		        break;
		    default:
		        //unknown  TODO: URGENT fix we should not have gotten here there is a TrieParse error which allows this to select the wrong tag!!
		    	logger.warn("Fix TrieParser query:   Connection: {}",TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, new StringBuilder()) );             
		    	
		    	//logger.warn("ERROR: {}",TrieParserReader.capturedFieldBytesAsUTF8Debug(trieReader, 0, new StringBuilder()) );
		    	return ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
		    	
		} 
		
		//TODO: add flag to keep this going if we find no conection??
		return requestContext;
	}


	private int keepAliveOrNotContext(int revisionId) {
		int requestContext = 0; //by default this is keep alive, eg zero.
        
        if(
           (HTTPRevisionDefaults.HTTP_0_9.ordinal() == revisionId) ||
           (HTTPRevisionDefaults.HTTP_1_0.ordinal() == revisionId)
          ) {
            requestContext |= ServerCoordinator.CLOSE_CONNECTION_MASK;
        }
		return requestContext;
	}


    private void assignMissingHeadersNull(Pipe<HTTPRequestSchema> staticRequestPipe, long headerMask, long basePos,
            byte[] offsets) {
        //write out null length for any missing fields
         int ord = 0;
         while (0!=headerMask) {                         
             if (0!=(1&headerMask)) {
                 System.err.println("target header missing, now filling with null");
                Pipe.setBytePosAndLen(Pipe.slab(staticRequestPipe), staticRequestPipe.mask, basePos+offsets[ord], Pipe.getBlobWorkingHeadPosition(staticRequestPipe), -1, Pipe.bytesWriteBase(staticRequestPipe));   
             }       
             ord++;        
         }
    }

    
    public static int[] buildEmptyBlockOfVarDatas(int fieldCount) {
        int[] result = new int[fieldCount*2];
        int i = fieldCount;
        int c = 0;
        while (--i >= 0) {
            result[c++] = 0;  //position
            result[c++] = -1; //null value length
        }       
        return result;
    }
    
    public static long addZeroLengthVarFields(Pipe targetOutput, int[] source) {
        PaddedLong workingHeadPos = Pipe.getWorkingHeadPositionObject(targetOutput);     
        Pipe.copyIntsFromToRing(source, 0, Integer.MAX_VALUE, Pipe.slab(targetOutput), (int)PaddedLong.get(workingHeadPos), Pipe.slabMask(targetOutput), source.length);
        long base = workingHeadPos.value;
        PaddedLong.add(workingHeadPos, source.length);
        return base;
    }   

    
}
