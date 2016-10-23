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
import com.ociweb.pronghorn.network.schema.ServerRequestSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.Pipe.PaddedLong;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Pool;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTPRouterStage<T extends Enum<T> & HTTPContentType,
                             H extends Enum<H> & HTTPHeaderKey,
                             R extends Enum<R> & HTTPRevision,
                             V extends Enum<V> & HTTPVerb> extends PronghornStage {

  //TODO: double check that this all works iwth ipv6.
    
    private static Logger logger = LoggerFactory.getLogger(HTTPRouterStage.class);
    
    private TrieParser verbMap;
    private TrieParser urlMap;
    private TrieParser headerMap;
    private TrieParser revisionMap;
        
    private TrieParserReader trieReader;
    
    private final Pipe<ServerRequestSchema>[] inputs;
    private       long[]                      inputChannels;
    private       int[]                       inputPositions;
    private       int[]                       inputLengths;
    private       boolean[]                   isOpen;
        
    
    private final Pipe<HTTPRequestSchema>[] outputs;
    private final Pipe errorPipe;
    private final int[]                     messageIds;
    private DataOutputBlobWriter<HTTPRequestSchema>[] blobWriter;
    private long[] requestHeaderMask;
    private byte[][] headerOffsets;
    private int[][] headerBlankBases;
    
    private static int MAX_HEADER = 1<<15;
    
    private final CharSequence[] paths;
    
    private int sequenceNo = 0;
    
    //TODO: need error if two strings are added to the same place?? or not
    //TODO: all the parts to the same message will share the same sequence no

    //NOTE: TODO: BBB for values mis-routed here we need a target stage to uprage connection and set the port
    
    private final HTTPSpecification<T,R,V,H> httpSpec;
        
    private int headerIdUpgrade    = HTTPHeaderKeyDefaults.UPGRADE.ordinal();
    private int headerIdConnection = HTTPHeaderKeyDefaults.CONNECTION.ordinal();
    private int headerIdContentLength = HTTPHeaderKeyDefaults.CONTENT_LENGTH.ordinal();
    
    private final int END_OF_HEADER_ID;
    private final int UNKNOWN_HEADER_ID;
    
    
    private int totalShortestRequest;
    
    //read all messages and they must have the same channelID
    //total all into one master DataInputReader
       
    
    public static HTTPRouterStage newInstance(GraphManager gm, Pool<Pipe<ServerRequestSchema>> input, Pipe<HTTPRequestSchema>[] outputs, Pipe errorResponse, 
                                              CharSequence[] paths, long[] headers, int[] messageIds) {
        
       return new HTTPRouterStage<HTTPContentTypeDefaults ,HTTPHeaderKeyDefaults, HTTPRevisionDefaults, HTTPVerbDefaults>(gm,input,outputs, errorResponse, paths, headers, messageIds,
                                   HTTPSpecification.defaultSpec()  ); 
    }

   
    public HTTPRouterStage(GraphManager gm, Pool<Pipe<ServerRequestSchema>> input, Pipe<HTTPRequestSchema>[] outputs, Pipe errorPipe,
                           CharSequence[] paths, long[] headers, int[] messageIds, 
                           HTTPSpecification<T,R,V,H> httpSpec) {
        super(gm,input.members(),join(outputs, errorPipe));
        this.inputs = input.members();
                
        this.outputs = outputs;
        this.errorPipe = errorPipe;
        this.messageIds = messageIds;
        this.requestHeaderMask = headers;
        
        this.httpSpec = httpSpec;
        
        this.paths = paths;
        
        END_OF_HEADER_ID  = httpSpec.headerCount+2;//for the empty header found at the bottom of the header
        UNKNOWN_HEADER_ID = httpSpec.headerCount+1;
        		
        supportsBatchedPublish = false;
        supportsBatchedRelease = false;
    }
    
    
    @Override
    public void startup() {
        
        //keeps the channel used by this pipe to ensure these are never mixed.
        //also used to release the subscription? TODO: this may be a race condition, not sure ...
        inputChannels = new long[inputs.length];
        Arrays.fill(inputChannels, -1);
        inputPositions = new int[inputs.length];
        inputLengths = new int[inputs.length];
        isOpen = new boolean[inputs.length];
        Arrays.fill(isOpen, true);
        
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
        
        ////
        ///
        
        urlMap = new TrieParser(1024,true);  //TODO: build size needed for these URLs dynamically 
        headerMap = new TrieParser(1024,true);//true skips deep checks we have an unknown value that gets picked for unexpected headers
        
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
        
        //Load the supported header keys
        H[] shr =  httpSpec.supportedHTTPHeaders.getEnumConstants();
        x = shr.length;
        while (--x >= 0) {
            //must have tail because the first char of the tail is required for the stop byte
            headerMap.setUTF8Value(shr[x].getKey(), "\n",shr[x].ordinal());
            headerMap.setUTF8Value(shr[x].getKey(), "\r\n",shr[x].ordinal());
        }     
        headerMap.setUTF8Value("\n", END_OF_HEADER_ID); //Detecting this first but not right!! we did not close the revision??
        headerMap.setUTF8Value("\r\n", END_OF_HEADER_ID);
    
        //TODO: this addition is slowing down the performance, must investigate
        headerMap.setUTF8Value("%b: %b\n", UNKNOWN_HEADER_ID); //TODO: bug in trie if we attemp to set this first...
        headerMap.setUTF8Value("%b: %b\r\n", UNKNOWN_HEADER_ID);        

        
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
        int i = inputs.length;
        int openCount = i;
        while (--i>=0) {
            openCount -= singlePipe(i);
        }
        if (0==openCount) {
            requestShutdown();
        } 
    }

    public int singlePipe(int idx) {
        Pipe<ServerRequestSchema> selectedInput = inputs[idx];     
        if (null==selectedInput) {
            return 1;
        }

        if (isOpen[idx]) {
            int messageIdx = accumulateRunningBytes(idx, selectedInput);
            if (messageIdx < 0) {
                isOpen[idx] = false;
            }
        }
        
        long channel   = inputChannels[idx];
        if (channel >= 0) {
            assert(inputLengths[idx]>=0) : "length is "+inputLengths[idx];
            TrieParserReader.parseSetup(trieReader, Pipe.blob(selectedInput), inputPositions[idx], inputLengths[idx], Pipe.blobMask(selectedInput));
            final int idx1 = idx;
            final long channel1 = channel; 
            
            consumeAvail(idx1, selectedInput, channel1, inputPositions[idx1], inputLengths[idx1]);
        }
        
        if (!isOpen[idx] && 0==inputLengths[idx]) {
            Pipe.publishWorkingTailPosition(selectedInput, Pipe.headPosition(selectedInput));//wipe out pipe.           
            inputs[idx] = null;
            return 1;
        }        
        return 0;
    }


    private void consumeAvail(final int idx, Pipe<ServerRequestSchema> selectedInput, final long channel, int p,
            int l) {
        int consumed;
        do {
            consumed = 0;
            final long toParseLength = TrieParserReader.parseHasContentLength(trieReader);
            assert(toParseLength>=0) : "length is "+toParseLength+" and input was "+l;
            
                        
          // logger.info("total input length "+toParseLength);
            

            if (toParseLength>=totalShortestRequest && route(trieReader, channel, idx)) {
                consumed = (int)(toParseLength - TrieParserReader.parseHasContentLength(trieReader));           
                Pipe.releasePendingAsReadLock(selectedInput, consumed);
                p = Pipe.blobMask(selectedInput) & (p + consumed);
                l -= consumed;                
                assert(l == trieReader.sourceLen);
                
            } else {
                assert(l>=0) : "length is "+l;
                break; //no room or no data try write again later.
            }
            
            if (l<=0) {
                inputChannels[idx] = -1;
                break;
            }
            
        } while (consumed>0);
        inputPositions[idx]=p;
        inputLengths[idx]=l;
    }


private boolean route(TrieParserReader trieReader, long channel, int idx) {    
    
	// TrieParserReader.debugAsUTF8(trieReader, System.out,3000, false);
	
	
    final int verbId = (int)TrieParserReader.parseNext(trieReader, verbMap);     //  GET /hello/x?x=3 HTTP/1.1 
  
    //TODO: compare with client...
    
    
    if (verbId<0) {
        if (TrieParserReader.parseHasContentLength(trieReader)<=0) { //TODO: how long until we timeout and abandon this partial data??
            //not error, must wait for more content and try again.
            return false;
        } else {
            
            //TODO: send error
            
        }
        
    }
    
  //  TrieParserReader.debugAsUTF8(trieReader, System.out,3000, false);
    final int routeId = (int)TrieParserReader.parseNext(trieReader, urlMap);     //  GET /hello/x?x=3 HTTP/1.1 
    if (routeId<0) {
        if (TrieParserReader.parseHasContentLength(trieReader)<=0) {
            //not error, must wait for more content and try again.
            return false;            
        } else {
            
            //TODO: send error
            
        }
    }
 
    Pipe<HTTPRequestSchema> staticRequestPipe = outputs[routeId];

    Pipe.markHead(staticRequestPipe);//holds in case we need to abandon our writes
    
    if (Pipe.hasRoomForWrite(staticRequestPipe) ) {
        
        final int size =  Pipe.addMsgIdx(staticRequestPipe, messageIds[routeId]);        // Write 1   1                         
        Pipe.addLongValue(channel, staticRequestPipe); // Channel                        // Write 2   3        
        Pipe.addIntValue(sequenceNo++, staticRequestPipe); //sequence                    // Write 1   4
        Pipe.addIntValue(verbId, staticRequestPipe);   // Verb                           // Write 1   5
        
        
        try {
        	//System.out.print("captured data:");
			TrieParserReader.writeCapturedValuesToAppendable(trieReader,System.out);
			//System.out.println();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        writeURLParamsToField(trieReader, blobWriter[routeId]);                          //write 2   7
       
        int httpRevisionId = (int)TrieParserReader.parseNext(trieReader, revisionMap);  //  GET /hello/x?x=3 HTTP/1.1 
        
        if (httpRevisionId<0) { 
            if (TrieParserReader.parseHasContentLength(trieReader)<=0) {
                //ROLL BACK THE WRITE
                Pipe.resetHead(staticRequestPipe);
                //not error, must wait for more content and try again.
                return false;            
            } else {
                 
                
                
                //TODO: send error
                
            }  

        }
        
        Pipe.addIntValue(httpRevisionId, staticRequestPipe); // Revision Id          // Write 1   8

        int requestContext = parseHeaderFields(routeId, staticRequestPipe, httpRevisionId, false);  // Write 2   10 //if header is presen
        if (ServerConnectionWriterStage.INCOMPLETE_RESPONSE_MASK == requestContext) {   
            //try again later, not complete.
            Pipe.resetHead(staticRequestPipe);
            return false;
        }        
        Pipe.addIntValue(requestContext, staticRequestPipe); // request context     // Write 1   11
        
        int consumed = Pipe.publishWrites(staticRequestPipe);                        // Write 1   12
        assert(consumed>=0);        
        Pipe.confirmLowLevelWrite(staticRequestPipe, size);
        
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


public boolean isCalled() {
    System.out.println("is called");
    return true;
}

private int accumulateRunningBytes(int idx, Pipe<ServerRequestSchema> selectedInput) {
    
    int messageIdx = Integer.MAX_VALUE;

    while (Pipe.hasContentToRead(selectedInput) && //NOTE has content to read looks at slab position between last read and new head.
            
            (hasNoActiveChannel(idx) ||      //if we do not have an active channel
             hasReachedEndOfStream(selectedInput) || //if we have reached the end of the stream
              hasContinuedData(idx, selectedInput) 
           )            
          ) {
        
        int guessLen = Pipe.peekInt(selectedInput,4);
        
        
        messageIdx = Pipe.takeMsgIdx(selectedInput);
        if (ServerRequestSchema.MSG_FROMCHANNEL_100 == messageIdx) {
            long channel   = Pipe.takeLong(selectedInput);
            
            int meta       = Pipe.takeRingByteMetaData(selectedInput);
            assert(Pipe.byteBackingArray(meta, selectedInput) == Pipe.blob(selectedInput));
            
            int length     = Pipe.takeRingByteLen(selectedInput);
            assert(length==guessLen);
            assert(length>0) : "value:"+length;
            assert(Pipe.validateVarLength(selectedInput, length));
            
            int pos        = Pipe.bytePosition(meta, selectedInput, length);                                            

            if (-1 == inputChannels[idx]) {
                //assign
                inputChannels[idx]  = channel;
                inputLengths[idx]   = length;
                inputPositions[idx] = pos;
     
                assert(inputLengths[idx]<selectedInput.sizeOfBlobRing);
            } else {
                //confirm match
                assert(inputChannels[idx] == channel) : "Internal error, mixed channels";
                   
                //grow position
                inputLengths[idx] += length; 
                
                assert(inputLengths[idx]<Pipe.blobMask(selectedInput)) : "When we roll up is must always be smaller than ring";
                
                //may only read up to safe point where head is
                assert(validatePipePositions(selectedInput, inputPositions[idx], inputLengths[idx]));
                                
            }
            
            //if we do not move this forward we will keep reading the same spot up to the new head
            Pipe.confirmLowLevelRead(selectedInput, Pipe.sizeOf(selectedInput, messageIdx)); 
            
            Pipe.readNextWithoutReleasingReadLock(selectedInput); 
            
        } else {
            assert(-1 == messageIdx);
            Pipe.confirmLowLevelRead(selectedInput, Pipe.EOF_SIZE);
            Pipe.readNextWithoutReleasingReadLock(selectedInput);
            return messageIdx;//do not loop again just exit now
        }
        
    }
 
    return messageIdx;
}


private boolean validatePipePositions(Pipe<ServerRequestSchema> selectedInput, int p, int l) {
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


private boolean hasContinuedData(int idx, Pipe<ServerRequestSchema> selectedInput) {
    return Pipe.peekLong(selectedInput, 1)==inputChannels[idx];
}


private boolean hasReachedEndOfStream(Pipe<ServerRequestSchema> selectedInput) {
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
                return ServerConnectionWriterStage.INCOMPLETE_RESPONSE_MASK; 
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
                requestContext = applyKeepAliveOrCloseToContext(requestContext);                
            }
            

            
            int maskId = 1<<headerId;
            
            if (0 == (maskId&headerMask) ) {
                //skip this data since the app module can not make use of it
                //this is the normal most frequent case                    
            } else {
            	
            	if (true) { //TODO: add support for this.
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
            return ServerConnectionWriterStage.INCOMPLETE_RESPONSE_MASK;
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
		switch(len) {
		    case 'l': //close
		        requestContext |= ServerConnectionWriterStage.CLOSE_CONNECTION_MASK;                        
		        break;
		    case 'e': //keep-alive
		        requestContext &= (~ServerConnectionWriterStage.CLOSE_CONNECTION_MASK);                        
		        break;
		    default:
		        //unknown
		    	logger.warn("connection :  {}",TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, new StringBuilder()) );                
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
            requestContext |= ServerConnectionWriterStage.CLOSE_CONNECTION_MASK;
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

    @Override
    public void shutdown() {
        
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
