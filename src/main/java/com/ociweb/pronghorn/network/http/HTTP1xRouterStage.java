package com.ociweb.pronghorn.network.http;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.SSLConnection;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTP1xRouterStage<T extends Enum<T> & HTTPContentType,
							   R extends Enum<R> & HTTPRevision,
						       V extends Enum<V> & HTTPVerb,
                               H extends Enum<H> & HTTPHeader
                             > extends PronghornStage {

    //TODO: double check that this all works iwth ipv6.
    
	private static final int SIZE_OF_BEGIN = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_BEGIN_208);
	private static final int SIZE_OF_PLAIN = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210);
	private static final int MAX_URL_LENGTH = 4096;
    private static Logger logger = LoggerFactory.getLogger(HTTP1xRouterStage.class);

    public static boolean showHeader = false; //set to true for debug to see headers in console.
    
    
	private static final boolean writeIndex = true; //must be on to ensure we can index to the header locations.
    private final int indexOffsetCount = 1;
    
    
    private TrieParserReader trieReader;
    private long activeChannel;
    
    private ErrorReporter errorReporter = new ErrorReporter() {

			@Override
			public boolean sendError(int errorCode) {				
				return HTTP1xRouterStage.this.sendError(activeChannel, idx-1, errorCode); 	
			}
     	
     };
     
    
    private final Pipe<NetPayloadSchema>[] inputs;
    private       long[]                   inputChannels;
    private       int[]                    inputBlobPos;
    private       int[]                    inputBlobPosLimit;
    
    private       int[]                    inputLengths;
    private       boolean[]                needsData;
    private       boolean[]                isOpen;
        
    private final Pipe<ReleaseSchema> releasePipe;
    
    private final Pipe<HTTPRequestSchema>[] outputs;
    
    private int   waitForOutputOn = -1;

    private long[] inputSlabPos;
    private int[] sequences;
    private int[] sequencesSent;
        
    public static int MAX_HEADER = 1<<15; //universal maximum header size.
    
    private int[] inputCounts;
    
    private int totalShortestRequest;
    private int shutdownCount;
    
    private int idx;
    private final HTTP1xRouterStageConfig<T,R,V,H> config;
    private final ServerCoordinator coordinator;
    private final Pipe<ServerResponseSchema> errorResponsePipe;
	private boolean catchAll;
    private final int parallelId;

    //read all messages and they must have the same channelID
    //total all into one master DataInputReader
       
    
    public static <	T extends Enum<T> & HTTPContentType,
					R extends Enum<R> & HTTPRevision,
					V extends Enum<V> & HTTPVerb,
					H extends Enum<H> & HTTPHeader> 
    
    HTTP1xRouterStage<T,R,V,H> newInstance(GraphManager gm, 
    									   int parallelId,
    		                               Pipe<NetPayloadSchema>[] input, 
    		                               Pipe<HTTPRequestSchema>[][] outputs, 
    		                               Pipe<ServerResponseSchema> errorResponsePipe,
    		                               Pipe<ReleaseSchema> ackStop,
                                           HTTP1xRouterStageConfig<T,R,V,H> config, 
                                           ServerCoordinator coordinator, boolean catchAll) {
        
       return new HTTP1xRouterStage<T,R,V,H>(gm,parallelId,input,outputs, errorResponsePipe, ackStop, config, coordinator, catchAll); 
    }
  
    
    public static <	T extends Enum<T> & HTTPContentType,
								R extends Enum<R> & HTTPRevision,
								V extends Enum<V> & HTTPVerb,
								H extends Enum<H> & HTTPHeader>
    
		HTTP1xRouterStage<T,R,V,H> newInstance(GraphManager gm, 
								   int parallelId,
		                           Pipe<NetPayloadSchema>[] input, 
		                           Pipe<HTTPRequestSchema>[] outputs,
		                           Pipe<ServerResponseSchema> errorResponsePipe, 
		                           Pipe<ReleaseSchema> ackStop,
		                           HTTP1xRouterStageConfig<T,R,V,H> config, 
		                           ServerCoordinator coordinator, boolean catchAll) {
		
		return new HTTP1xRouterStage<T,R,V,H>(gm,parallelId,input,outputs, errorResponsePipe, ackStop, config, coordinator, catchAll); 
	}

	public HTTP1xRouterStage(GraphManager gm, 
			int parallelId,
            Pipe<NetPayloadSchema>[] input, Pipe<HTTPRequestSchema>[][] outputs, 
            Pipe<ServerResponseSchema> errorResponsePipe, Pipe<ReleaseSchema> ackStop,
            HTTP1xRouterStageConfig<T,R,V,H> config, ServerCoordinator coordinator, boolean catchAll) {
		
		this(gm, parallelId, input, join(outputs), errorResponsePipe, ackStop, config, coordinator, catchAll);
		
		int inMaxVar = PronghornStage.maxVarLength(input);		
		int outMaxVar =  PronghornStage.minVarLength(outputs);
		
		if (outMaxVar <= inMaxVar) {
			throw new UnsupportedOperationException("Input has field lenght of "+inMaxVar+" while output pipe is "+outMaxVar+", output must be larger");
		}
		GraphManager.addNota(gm, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
		
	}
	public HTTP1xRouterStage(GraphManager gm, 
			                 int parallelId,
			                 Pipe<NetPayloadSchema>[] input, 
			                 Pipe<HTTPRequestSchema>[] outputs,
			                 Pipe<ServerResponseSchema> errorResponsePipe, Pipe<ReleaseSchema> ackStop,
                             HTTP1xRouterStageConfig<T,R,V,H> config, ServerCoordinator coordinator, boolean catchAll) {
		
        super(gm,input,join(outputs,ackStop,errorResponsePipe));
        
        this.parallelId = parallelId;
        
        assert(outputs!=null);
        assert(outputs.length>0);
        
        this.config = config;
        this.inputs = input;
        this.releasePipe = ackStop;        
        this.outputs = outputs;
        this.coordinator = coordinator;
        this.errorResponsePipe = errorResponsePipe;
        this.catchAll = catchAll;
        assert(outputs.length>=0) : "must have some target for the routed REST calls";
        
        this.shutdownCount = inputs.length;
        this.supportsBatchedPublish = false;
        this.supportsBatchedRelease = false;
        
        GraphManager.addNota(gm, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
		
    }    
	
    
    @Override
    public void startup() {
        
    	idx = inputs.length;
    	
    	inputCounts = new int[inputs.length];
    	
        //keeps the channel used by this pipe to ensure these are never mixed.
        //also used to release the subscription? TODO: this may be a race condition, not sure ...
        inputChannels = new long[inputs.length];
        Arrays.fill(inputChannels, -1);
        inputBlobPos = new int[inputs.length];
        inputBlobPosLimit = new int[inputs.length];
        inputLengths = new int[inputs.length];
        needsData = new boolean[inputs.length];
        isOpen = new boolean[inputs.length];
        Arrays.fill(isOpen, true);
        
        inputSlabPos = new long[inputs.length];
        sequences = new int[inputs.length];
        sequencesSent = new int[inputs.length];
        
        final int sizeOfVarField = 2;
 
        int h = config.totalPathsCount();
        
        while (--h>=0) { 
            byte[] offsets = new byte[config.httpSpec.headerCount+1];
            byte runningOffset = 0;
            
            IntHashTable table = config.headerToPositionTable(h);
            
            for(int ordinalValue = 0; ordinalValue<=config.httpSpec.headerCount; ordinalValue++) {
                //only set fields for those which are on, and do in this order.
                if (IntHashTable.hasItem(table, HTTPHeader.HEADER_BIT | ordinalValue)) {
                    offsets[ordinalValue] = runningOffset;
                    runningOffset += sizeOfVarField;
                }
            }
               
        }

        ///
        

        trieReader = new TrieParserReader(16);//max fields we support capturing.
        
        totalShortestRequest = 0;//count bytes for the shortest known request, this opmization helps prevent parse attempts when its clear that there is not enough data.

        //a request may have NO header with just the end marker so add one
        totalShortestRequest+=1;
                

        totalShortestRequest += config.revisionMap.shortestKnown();
        totalShortestRequest += config.verbMap.shortestKnown();        
        totalShortestRequest += config.urlMap.shortestKnown(); 
        
        if (totalShortestRequest<=0) {
            totalShortestRequest = 1;
        }        
        
    }    

    @Override
    public void shutdown() {
        
    	if (shutdownCount>0) {
    		logger.trace("warning, forced shutdown of "+getClass().getSimpleName()+" while its still waiting for {}", shutdownCount);
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
    	
        if (waitForOutputOn>=0 && waitForOutputOn<outputs.length) {
        	
        	if (Pipe.hasRoomForWrite(outputs[waitForOutputOn])) {
        		waitForOutputOn=-1;
        	} else {
        		//logger.trace("wait for output on {}",waitForOutputOn);
        		return;//output is backed up so go do something else.
        	}
        }
    	
    	int didWork;
   	
    	do { 
    		didWork = 0;
	   
    		int localIdx = idx;
    		
    		int m = 100;//max iterations before taking a break
    		do {
		        while (--localIdx>=0 && --m>=0) {
		            int result = singlePipe(localIdx);
		            
		            if (result<0) {
		            	if (--shutdownCount<=0) {
		            		requestShutdown();
		            		return;
		            	}
		            	
		            } else {	            		            	
		            	didWork+=result;
		            }
		            
		            if (waitForOutputOn>=0) { //abandon until the output pipe is cleared.
		            	idx = localIdx;
		            	return;
		            } 
		            
		            
		        }
		        if (localIdx<=0) {
		        	localIdx = inputs.length;
		        }
    		} while ((--m>=0) && (didWork!=0));
    		idx = localIdx;
    	} while (didWork!=0);    

    }

    
    //return 0, 1 work, -1 shutdown.
    public int singlePipe(final int idx) {
        Pipe<NetPayloadSchema> selectedInput = inputs[idx];     

        if (isOpen[idx]) {  

        	//logger.info("accum off this pipe "+isOpen[idx]+"   "+inputChannels[idx]);
        	int[] localInputLengths = inputLengths;
        	final int start = localInputLengths[idx];
            if (accumulateRunningBytes(idx, selectedInput) < 0) {//message idx
            	//logger.trace("detected EOF for {}",idx);
            	//accumulate these before shutdown?? also wait for all data to be consumed.
                isOpen[idx] = false;
                if (localInputLengths[idx]<=0) {
                	return -1;
                }
            }
            
            if (needsData[idx]) {
	            if (localInputLengths[idx]==start) {
	            	//we got no data so move on to the next
	            	return 0;
	            } else {
	            	needsData[idx]=false;
	            }
            }            
        }

        return ((activeChannel = inputChannels[idx]) >= 0) ?
	        	(parseAvail(idx, selectedInput, activeChannel) ? 1 : 0) : 0;
    }


	private boolean parseAvail(final int idx, Pipe<NetPayloadSchema> selectedInput, final long channel) {
		
		boolean didWork = false;
			
		boolean webSocketUpgraded = ServerCoordinator.isWebSocketUpgraded(coordinator, channel);
		if (!webSocketUpgraded) {			
			didWork = parseHTTPAvail(idx, selectedInput, channel, didWork);
		} else {
			
			final int totalAvail = inputLengths[idx]; 
		    
            final int pipeIdx = ServerCoordinator.getWebSocketPipeIdx(coordinator, channel);
            final Pipe<HTTPRequestSchema> outputPipe = outputs[pipeIdx];
            if (!Pipe.hasRoomForWrite(outputPipe) ) {
             	return markBlockConsumed(idx, selectedInput, channel, 
		                   inputBlobPos[idx], 
		                   totalAvail, -pipeIdx, //negative pipeIdx indicates the output is backed up. 
		                   0, totalAvail);
            }
			
			final byte[] backing = Pipe.blob(selectedInput);
            final int mask = Pipe.blobMask(selectedInput);
          
            if (totalAvail < 2) {
            	return markBlockConsumed(idx, selectedInput, channel, 
            			                   inputBlobPos[idx], 
            			                   totalAvail, NEED_MORE_DATA, 0, totalAvail);
            }
            
            int pos = inputBlobPos[idx];
            
            int finOpp = backing[mask & pos++];
            int b2 = backing[mask & pos++];

            int headerSize = 2;

            int msk = b2&1;
            long length = (b2>>1);
            
            if (length<126) {
            	//small 7 bits
            	//correct as is
            
            } else if (length==126){
            	//med 16 bits
            	length = ((       backing[mask & pos++] << 8) |
                        (0xFF & backing[mask & pos++])); 
            	headerSize += 2;
            } else {
            	//large 64 bits
            	length =
            	( ( (  (long)backing[mask & pos++]) << 56) |              
                        ( (0xFFl & backing[mask & pos++]) << 48) |
                        ( (0xFFl & backing[mask & pos++]) << 40) |
                        ( (0xFFl & backing[mask & pos++]) << 32) |
                        ( (0xFFl & backing[mask & pos++]) << 24) |
                        ( (0xFFl & backing[mask & pos++]) << 16) |
                        ( (0xFFl & backing[mask & pos++]) << 8) |
                          (0xFFl & backing[mask & pos++]) ); 
            	headerSize += 8;
            }
            if (totalAvail < (2+length+(msk<<2))) {
            	return markBlockConsumed(idx, selectedInput, channel, 
		                   inputBlobPos[idx], 
		                   totalAvail, NEED_MORE_DATA, 0, totalAvail);
            }
       
            /////////////////
            //at this point we are committed to doing the write
            //we have data and we have room
            ///////////////////
            didWork = true;
			int size = Pipe.addMsgIdx(outputPipe, HTTPRequestSchema.MSG_WEBSOCKETFRAME_100 );
			Pipe.addLongValue(channel, outputPipe);
			Pipe.addIntValue(sequences[idx], outputPipe);
			Pipe.addIntValue(finOpp, outputPipe);

        	assert(length<=Integer.MAX_VALUE);
        	
            int maskPosition = 0; 
            DataOutputBlobWriter<HTTPRequestSchema> stream;
            if (1==msk) {//masking is the normal condition for almost all calls
            	maskPosition = pos;//keep for use later
            	
            	//make this value available, as well to the caller
            	int maskValue =	 ( ( (            backing[mask & pos++]) << 24) |
				            			( (0xFF & backing[mask & pos++]) << 16) |
				            			( (0xFF & backing[mask & pos++]) << 8) |
				            			  (0xFF & backing[mask & pos++]) );
            	headerSize += 4;
            	Pipe.addIntValue(maskValue, outputPipe);
            	//un-mask the data into the next pipe for use
            	
            	stream = Pipe.openOutputStream(outputPipe);
            	
            	for(int i = 0; i<length; i++) {            		
            		stream.writeByte(backing[mask & ((maskPosition+i) & 0x3)] ^ backing[mask & pos++] );
            	}            	
            } else {
            	Pipe.addIntValue(0, outputPipe);      	//no mask discoverd so directly copy the data
            	
            	stream = Pipe.openOutputStream(outputPipe);
            	stream.write(backing, pos, (int)length, mask);
            }

            DataOutputBlobWriter.closeLowLevelField(stream);
            	
        	Pipe.confirmLowLevelWrite(outputPipe, size);
        	Pipe.publishWrites(outputPipe);
        	
        	int totalConsumed = (int)(length+headerSize);
			return markBlockConsumed(  idx, selectedInput, channel, 
					                   inputBlobPos[idx], 
					                   totalAvail, 
					                   NEED_MORE_DATA, 
					                   totalConsumed, 
					                   totalAvail-totalConsumed); // remaining bytes
           	
		}
				
		return didWork;
	}


	private boolean parseHTTPAvail(final int idx, Pipe<NetPayloadSchema> selectedInput, final long channel,
			boolean didWork) {
		boolean result;
		//NOTE: we start at the same position until this gets consumed
		TrieParserReader.parseSetup(trieReader, 
				                    Pipe.blob(selectedInput), 
				                    inputBlobPos[idx], 
				                    inputLengths[idx], 
				                    Pipe.blobMask(selectedInput));	

		do {
			assert(inputLengths[idx]>=0) : "length is "+inputLengths[idx];

			int totalAvail = inputLengths[idx];
			final long toParseLength = TrieParserReader.parseHasContentLength(trieReader);
			assert(toParseLength>=0) : "length is "+toParseLength+" and input was "+totalAvail;
			            
			if (toParseLength>0) {
				
				int state = parseHTTP(trieReader, channel, idx);				
				int totalConsumed = (int)(toParseLength - TrieParserReader.parseHasContentLength(trieReader));           
				int remainingBytes = trieReader.sourceLen;
				
				result = markBlockConsumed(idx, selectedInput, channel, inputBlobPos[idx], totalAvail, state, totalConsumed, remainingBytes);
				didWork|=result;
			} else {
			
				assert(inputLengths[idx] <= (selectedInput.blobMask+(selectedInput.blobMask>>1))) : "This data should have been parsed but was not understood, check parser and/or sender for corruption.";
				result = false;
			} 
		   	           
		} while (result);
		return didWork;
	}




	private boolean markBlockConsumed(final int idx, Pipe<NetPayloadSchema> selectedInput, final long channel, int p,
			int totalAvail, final int result, int totalConsumed, int remainingBytes) {
		if (SUCCESS == result) {
			
		    p += totalConsumed;
		    totalAvail -= totalConsumed;

		    Pipe.releasePendingAsReadLock(selectedInput, totalConsumed);

		    assert(totalAvail == remainingBytes);
		    inputBlobPos[idx]=p;

		    assert(boundsCheck(idx, totalAvail));
		    
		    inputLengths[idx] = totalAvail;	                
		                
		    //the release pending above should keep them in algnment and the ounstanding should match
		    assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));
		    
		    if (totalAvail==0) {
		    	inputChannels[idx] = -1;//must clear since we are at a safe release point
		    	                        //next read will start with new postion.
		    		//send release if appropriate
       
		    		if ((remainingBytes<=0) 
		    			&& consumedAllOfActiveFragment(selectedInput, p)) { 
		    	
		    				assert(0==Pipe.releasePendingByteCount(selectedInput));
							sendRelease(channel, idx);
		    														
					}
		    }
		    return totalConsumed>0;
         } else if (NEED_MORE_DATA == result) {
		
		   needsData[idx]=true;      	   //TRY AGAIN AFTER WE PARSE MORE DATA IN.
		   return false;//this is the only way to pick up more data, eg. exit the outer loop with zero.
		   
         } else {
		   //TRY AGAIN AFTER PIPE CLEARS
		   waitForOutputOn = (-result);
		   return false;//exit to take a break while pipe is full.
         }
	}

	private boolean boundsCheck(final int idx, int l) {
		if (inputBlobPos[idx]>inputBlobPosLimit[idx]) {
			new Exception("position is out of bounds "+inputBlobPos[idx]+" vs "+inputBlobPosLimit[idx]).printStackTrace();;
		}
		if (l<0) {
			new Exception("can not consume more than length "+l).printStackTrace();
		}
		return true;
	}


	private static boolean consumedAllOfActiveFragment(Pipe<NetPayloadSchema> selectedInput, int p) {

		return (Pipe.blobMask(selectedInput)&Pipe.getWorkingBlobRingTailPosition(selectedInput)	) == (Pipe.blobMask(selectedInput)&p) && 
		       (Pipe.blobMask(selectedInput)&Pipe.getWorkingBlobHeadPosition(selectedInput)	) == (Pipe.blobMask(selectedInput)&p);
		
	}

    
 private final static int NEED_MORE_DATA = 2;
 private final static int SUCCESS = 1;
 
// -1 no room on output pipe (do not call again until the pipe is clear, send back pipe to watch)
//  2 need more data to parse (do not call again until data to parse arrives)
//  1 success
// <=0 for wating on this output pipe to have room (the pipe idx is negative)
 
 
private int parseHTTP(TrieParserReader trieReader, final long channel, final int idx) {    

    if (showHeader) {
    	System.out.println("///////////////// ROUTE HEADER "+channel+"///////////////////");
    	TrieParserReader.debugAsUTF8(trieReader, System.out, Math.min(8192, trieReader.sourceLen), false); //shows that we did not get all the data
    	if (trieReader.sourceLen>8192) {
    		System.out.println("...");
    	}
    	System.out.println("\n///////////////////////////////////////////");
    }

	int tempLen = trieReader.sourceLen;
	int tempPos = trieReader.sourcePos;
    
	if (tempLen<=0) {
		return NEED_MORE_DATA;
	}
	
	final int verbId = (int)TrieParserReader.parseNext(trieReader, config.verbMap);     //  GET /hello/x?x=3 HTTP/1.1     
    if (verbId<0) {
    		if (tempLen < (config.verbMap.longestKnown()+1) || (trieReader.sourceLen<0)) { //added 1 for the space which must appear after
    			return NEED_MORE_DATA;    			
    		} else {
    			logger.info("bad HTTP data recieved by server, channel will be closed.");
    			sendError(trieReader, channel, idx, tempLen, tempPos, 400);	
    			
    			//we have bad data we have been sent, there is enough data yet the verb was not found
    			
    			boolean debug = false;
    			if(debug) {
    				trieReader.sourceLen = tempLen;
    				trieReader.sourcePos = tempPos;
    				StringBuilder builder = new StringBuilder();    			    			
    				TrieParserReader.debugAsUTF8(trieReader, builder, config.verbMap.longestKnown()*2);    			
    				logger.warn("{} looking for verb but found:\n{} at position {} \n\n",channel,builder,tempPos);
    			}
    			
    			trieReader.sourceLen = 0;
    			trieReader.sourcePos = 0;    			
    			
    			SSLConnection con = coordinator.connectionForSessionId(channel);
				if (null!=con) {
					con.clearPoolReservation();
				}
    			//logger.info("success");
    			return SUCCESS;
    			    		    			
    		}
    		
    }
   // System.err.println("start at pos "+tempPos+" for "+channel);
    
	tempLen = trieReader.sourceLen;
	tempPos = trieReader.sourcePos;
	final int pathId = (int)TrieParserReader.parseNext(trieReader, config.urlMap);     //  GET /hello/x?x=3 HTTP/1.1 
    //the above URLS always end with a white space to ensure they match the spec.
    if (!catchAll && config.UNMAPPED_ROUTE == pathId) {
    	
		//unsupported route path, send 404 error
		sendError(trieReader, channel, idx, tempLen, tempPos, 404);	
		return SUCCESS;
    }
    
    if (pathId<0) {

    	if (tempLen < config.urlMap.longestKnown() || trieReader.sourceLen<0) {
    		//logger.info(routeId+" need more data C  "+tempLen+"  "+config.urlMap.longestKnown()+" "+trieReader.sourceLen);
			return NEED_MORE_DATA;    			
		} else {
			//bad format route path error, could not find space after path and before route, send 404 error
			sendError(trieReader, channel, idx, tempLen, tempPos, 404);	
			return SUCCESS;
		}
    }
 
   // logger.info("send this message to route {}",routeId);
    
    //if thie above code went past the end OR if there is not enough room for an empty header  line maker then return
    if (trieReader.sourceLen<2) {
    	//logger.info("need more data D");
    	return NEED_MORE_DATA;
    }    	
    
    //NOTE: many different routeIds may return the same outputPipe, since they all go to the same palace
    //      if catch all is enabled use it because all outputs will be null in that mode
    Pipe<HTTPRequestSchema> outputPipe = pathId<outputs.length ? outputs[pathId] : outputs[0];
    Pipe.markHead(outputPipe);//holds in case we need to abandon our writes
    
    if (Pipe.hasRoomForWrite(outputPipe) ) {

        final int size =  Pipe.addMsgIdx(outputPipe, HTTPRequestSchema.MSG_RESTREQUEST_300);        // Write 1   1                         
        Pipe.addLongValue(channel, outputPipe); // Channel                        // Write 2   3        
        Pipe.addIntValue(sequences[idx], outputPipe); //sequence                    // Write 1   4
        
        //route and verb
        Pipe.addIntValue((pathId << HTTPVerb.BITS) | (verbId & HTTPVerb.MASK), outputPipe);// Verb                           // Write 1   5
        
		DataOutputBlobWriter<HTTPRequestSchema> writer = Pipe.outputStream(outputPipe);
		                                                                                                                          //write 2   7
        DataOutputBlobWriter.openField(writer); //the beginning of the payload always starts with the URL arguments

        if (writeIndex) {
        	//this is writing room for the position of the body later.
        	int i = indexOffsetCount; //provide room for these (if there is more than one)
        	while (--i>=0) {
        		
				if (!DataOutputBlobWriter.tryWriteIntBackData(writer, 0)) {//we write zero for now
					throw new UnsupportedOperationException("Pipe var field length is too short for "+DataOutputBlobWriter.class.getSimpleName()+" change config for "+writer.getPipe());
				}
        	}
        }
        
        
        try {
		    TrieParserReader.writeCapturedValuesToDataOutput(trieReader, writer, writeIndex);
		} catch (IOException e) {        
		    //this exception should never happen, will not throw writing to field not stream
			throw new RuntimeException(e); 
		}
		

    	tempLen = trieReader.sourceLen;
    	tempPos = trieReader.sourcePos;
        int httpRevisionId = (int)TrieParserReader.parseNext(trieReader, config.revisionMap);  //  GET /hello/x?x=3 HTTP/1.1 
        
        if (httpRevisionId<0) { 
        	DataOutputBlobWriter.closeLowLevelField(writer);
        	Pipe.resetHead(outputPipe);
        	if (tempLen < (config.revisionMap.longestKnown()+1) || (trieReader.sourceLen<0) ) { //added 1 for the space which must appear after
        		//logger.info("need more data D");        		
        		return NEED_MORE_DATA;    			
    		} else {
    			logger.info("bad HTTP data recieved by server, channel will be closed.");
    			sendError(trieReader, channel, idx, tempLen, tempPos, 400);	
    			
    			boolean debug = false;
    			if (debug) {
	    			//we have bad data we have been sent, there is enough data yet the revision was not found
	    			trieReader.sourceLen = tempLen;
	    			trieReader.sourcePos = tempPos;
	    			
	    			StringBuilder builder = new StringBuilder();
	    			TrieParserReader.debugAsUTF8(trieReader, builder, config.revisionMap.longestKnown()*4);
	    			//logger.warn("{} looking for HTTP revision but found:\n{}\n\n",channel,builder);
    			}
    			trieReader.sourceLen = 0;
    			trieReader.sourcePos = 0;
    			
    			SSLConnection con = coordinator.connectionForSessionId(channel);
				if (null!=con) {
					con.clearPoolReservation();
				}
    			//logger.info("success");
    			return SUCCESS;
    		}
        }
        
		////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////also write the requested headers out to the payload
        /////////////////////////////////////////////////////////////////////////
        
        //logger.info("extractions before headers count is {} ",config.extractionParser(routeId).getIndexCount());
        
        int countOfAllPreviousFields = config.extractionParser(pathId).getIndexCount()+indexOffsetCount;
		int requestContext = parseHeaderFields(writer, errorReporter, 
											 countOfAllPreviousFields, 
											 config.headerCount(pathId),
											 config.headerToPositionTable(pathId), 
											 writeIndex, 
											 keepAliveOrNotContext(httpRevisionId),
											 trieReader,
											 config.headerMap,
											 config.END_OF_HEADER_ID);  // Write 2   10 //if header is presen
       
        
        if (ServerCoordinator.INCOMPLETE_RESPONSE_MASK == requestContext) {  
        	DataOutputBlobWriter.closeLowLevelField(writer);
            //try again later, not complete.
            Pipe.resetHead(outputPipe);
            return NEED_MORE_DATA;
        } 
        
   
        ////////TODO: URGENT: re-think this as we still need to add the chunked post payload??
        
    	DataOutputBlobWriter.commitBackData(writer);
    	DataOutputBlobWriter.closeLowLevelField(writer);
        
        
		//not an error we just looked past the end and need more data
	    if (trieReader.sourceLen<0) {
	    	Pipe.resetHead(outputPipe);
		    return NEED_MORE_DATA;
		} 

        //NOTE: we must close the writer for the params field before we write the parallelId and  revision 
	    Pipe.addIntValue((parallelId << HTTPRevision.BITS) | (httpRevisionId & HTTPRevision.MASK), outputPipe);// Revision Id          // Write 1 
        Pipe.addIntValue(requestContext, outputPipe); // request context      // Write 1 
        
        int consumed = Pipe.publishWrites(outputPipe);                        // Write 1 
        assert(consumed>=0);        
        Pipe.confirmLowLevelWrite(outputPipe, size); 
  
        sequences[idx]++; //increment the sequence since we have now published the route.
        
    } else {
    	//logger.info("No room, waiting for {} {}",channel, outputPipe);
        //no room try again later
        return -pathId;
    }
    
   inputCounts[idx]++; 
 //  assert(validateNextByte(trieReader, idx));
   return SUCCESS;
}

 void sendError(TrieParserReader trieReader, final long channel, final int idx, int tempLen, int tempPos,
		int errorCode) {
	boolean sent = sendError(channel, idx, errorCode); 			
	
	if (!sent) {
		trieReader.sourceLen = tempLen;
		trieReader.sourcePos = tempPos;
		
		StringBuilder builder = new StringBuilder();
		TrieParserReader.debugAsUTF8(trieReader, builder, MAX_URL_LENGTH);			
		logger.warn("Unable to send {}, too many errors, found unrecognized route:\n\"{}\"\n\n",errorCode, builder);
		//close this connection now since we could not respond.
		//do not cooperate with the client if we have an invalid path.
		//this must be fixed on the client side.
		////////////////
		//this block is already done because sendError will close upon xmit
		//it is doen here because the sendError failed
		SSLConnection con = coordinator.connectionForSessionId(channel);
		if (null!=con) {
			con.clearPoolReservation();		
			con.close();
		}
	}
	//in all cases clear out the reader data.
	//this connection is now closed so do not read any more.
	trieReader.sourceLen = 0;
	trieReader.sourcePos = 0;
		
}

private boolean sendError(final long channel, final int idx, int errorCode) {
	boolean sent = false;
	if (Pipe.hasRoomForWrite(errorResponsePipe)) {
		//will close connection as soon as error is returned.
		HTTPUtil.publishStatus(channel, sequences[idx], 
				              errorCode, errorResponsePipe);		
		sent = true;				
	}
	return sent;
}

private boolean validateNextByte(TrieParserReader trieReader, int idx) {
	StringBuilder temp = new StringBuilder();
	TrieParserReader.debugAsUTF8(trieReader, temp, 4,false);
	boolean expr = trieReader.sourceLen<=0 || temp.charAt(0)=='G';
	assert expr :"bad first bytes detected as "+temp+" on input count "+inputCounts[idx];
	return true;
}

private void sendRelease(long channel, final int idx) {

	assert(channel>=0) : "channel must exist";
	assert(inputSlabPos[idx]>=0);
	Pipe.presumeRoomForWrite(releasePipe);
	
	int s = Pipe.addMsgIdx(releasePipe, ReleaseSchema.MSG_RELEASEWITHSEQ_101);
	Pipe.addLongValue(channel, releasePipe);
	Pipe.addLongValue(inputSlabPos[idx], releasePipe);
	Pipe.addIntValue(sequences[idx], releasePipe); //send current sequence number so others can continue at this count.
	sequencesSent[idx] = sequences[idx];
	
	
	Pipe.confirmLowLevelWrite(releasePipe, s);
	Pipe.publishWrites(releasePipe);
	this.inputSlabPos[idx]=-1;

}

private void badClientError(long channel) {
	SSLConnection con = coordinator.connectionForSessionId(channel);
	if (null!=con) {
		con.clearPoolReservation();		
		con.close();
	}
}


private int accumulateRunningBytes(final int idx, Pipe<NetPayloadSchema> selectedInput) {
    
    int messageIdx = Integer.MAX_VALUE;
    long inChnl = inputChannels[idx];

    
//    boolean debug = false;
//    if (debug) {
//		logger.info("{} accumulate data rules {} && ({} || {} || {})", idx,
//	    		     Pipe.hasContentToRead(selectedInput), 
//	    		     hasNoActiveChannel(inChnl), 
//	    		     hasReachedEndOfStream(selectedInput), 
//	    		     hasContinuedData(selectedInput, inChnl) );
//    }

    
    while ( //NOTE has content to read looks at slab position between last read and new head.
   
    		Pipe.hasContentToRead(selectedInput) && (    //content checked first to ensure asserts pass		
    				hasNoActiveChannel(inChnl) ||      //if we do not have an active channel
    				hasContinuedData(selectedInput, inChnl) ||
    				hasReachedEndOfStream(selectedInput) 
    				//if we have reached the end of the stream       
            )
          ) {

        
        messageIdx = Pipe.takeMsgIdx(selectedInput);
        
        //logger.info("seen message id of {}",messageIdx);
        
        if (NetPayloadSchema.MSG_PLAIN_210 == messageIdx) {
            inChnl = processPlain(idx, selectedInput, inChnl);
        } else {
        	if (NetPayloadSchema.MSG_BEGIN_208 == messageIdx) {        		
        		processBegin(idx, selectedInput);        		
        	} else {
	            return processShutdown(selectedInput, messageIdx);
        	}
        }        
    }
    

 
    return messageIdx;
}


private long processPlain(final int idx, Pipe<NetPayloadSchema> selectedInput, long inChnl) {
	final long channel = Pipe.takeLong(selectedInput);
	final long arrivalTime = Pipe.takeLong(selectedInput);
	
	long slabPos;
	this.inputSlabPos[idx] = slabPos = Pipe.takeLong(selectedInput);            
        
	final int meta       = Pipe.takeRingByteMetaData(selectedInput);
	final int length     = Pipe.takeRingByteLen(selectedInput);
	final int pos        = Pipe.bytePosition(meta, selectedInput, length);                                            
	
	assert(Pipe.byteBackingArray(meta, selectedInput) == Pipe.blob(selectedInput));            
	assert(length<=selectedInput.maxVarLen);
	assert(inputBlobPos[idx]<=inputBlobPosLimit[idx]) : "position is out of bounds.";
	
	if (-1 != inChnl) {
		plainMatch(idx, selectedInput, channel, length);
	} else {
		inChnl = plainFreshStart(idx, selectedInput, channel, length, pos);		
	}

	assert(inputLengths[idx]>=0) : "error negative length not supported";

	//if we do not move this forward we will keep reading the same spot up to the new head
	Pipe.confirmLowLevelRead(selectedInput, SIZE_OF_PLAIN);            
	Pipe.readNextWithoutReleasingReadLock(selectedInput); 
		
	if (-1 == slabPos) {
		inputSlabPos[idx] = Pipe.getWorkingTailPosition(selectedInput); //working and was tested since this is low level with unrleased block.
	}
	assert(inputSlabPos[idx]!=-1);
	return inChnl;
}


private void plainMatch(final int idx, Pipe<NetPayloadSchema> selectedInput, long channel, int length) {
	//confirm match
	assert(inputChannels[idx] == channel) : "Internal error, mixed channels";
	
	//grow position
	assert(inputLengths[idx]>0) : "not expected to be 0 or negative but found "+inputLengths[idx];
	inputLengths[idx] += length; 
	inputBlobPosLimit[idx] += length;
	
	assert(inputLengths[idx] < Pipe.blobMask(selectedInput)) : "When we roll up is must always be smaller than ring "+inputLengths[idx]+" is too large for "+Pipe.blobMask(selectedInput);
	
	//may only read up to safe point where head is       
	assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));
}


private long plainFreshStart(final int idx, Pipe<NetPayloadSchema> selectedInput, long channel, int length, int pos) {
	long inChnl;
	//is freshStart
	assert(inputLengths[idx]<=0) : "expected to be 0 or negative but found "+inputLengths[idx];
	
	//assign
	inputChannels[idx]     = inChnl = channel;
	inputLengths[idx]      = length;
	inputBlobPos[idx]      = pos;
	inputBlobPosLimit[idx] = pos + length;
	
	//logger.info("added new fresh start data of {}",length);
	
	assert(inputLengths[idx]<selectedInput.sizeOfBlobRing);
	assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));
	return inChnl;
}


private int processShutdown(Pipe<NetPayloadSchema> selectedInput, int messageIdx) {
	assert(-1 == messageIdx) : "messageIdx:"+messageIdx;
	if (-1 != messageIdx) {
		throw new UnsupportedOperationException("bad id "+messageIdx+" raw data  \n"+selectedInput);
	}
	
	Pipe.confirmLowLevelRead(selectedInput, Pipe.EOF_SIZE);
	Pipe.readNextWithoutReleasingReadLock(selectedInput);
	return messageIdx;//do not loop again just exit now
}


private void processBegin(final int idx, Pipe<NetPayloadSchema> selectedInput) {
	assert(hasNoActiveChannel(inputChannels[idx])) : "Can not begin a new connection if one is already in progress.";        		
	assert(0==Pipe.releasePendingByteCount(selectedInput));
		     
	     //  	  ServerCoordinator.inServerCount.incrementAndGet();
       // 	  ServerCoordinator.start = System.nanoTime();
     	  
	//logger.info("accumulate begin");
	//keep this as the base for our counting of sequence
	sequences[idx] = Pipe.takeInt(selectedInput);
	
	Pipe.confirmLowLevelRead(selectedInput, SIZE_OF_BEGIN);
	//Pipe.releaseReadLock(selectedInput);
	Pipe.readNextWithoutReleasingReadLock(selectedInput);
	//do not return, we will go back arround the while again. 
}


    // Warning Pipe.hasContentToRead(selectedInput) must be called first.
	private boolean hasNoActiveChannel(long inChnl) {
		return -1 == inChnl;
	}

	// Warning Pipe.hasContentToRead(selectedInput) must be called first.
	private boolean hasContinuedData(Pipe<NetPayloadSchema> selectedInput, long inChnl) {
		return Pipe.peekLong(selectedInput, 1)==inChnl;
	}
	
	// Warning Pipe.hasContentToRead(selectedInput) must be called first.
	private static boolean hasReachedEndOfStream(Pipe<NetPayloadSchema> selectedInput) {
	    return Pipe.peekInt(selectedInput)<0;
	}
	
	

    protected static int parseHeaderFields(DataOutputBlobWriter<HTTPRequestSchema> writer, //copy data to here
						            ErrorReporter errorReporter,
									final int indexOffsetCount, //previous fields already written
									final int headerCount, 		//total headers known 
									final IntHashTable headerToPositionTable, //which headers do we want to capture/index
									final boolean writeIndex,   //yes we should index these headers
									int requestContext,
									final TrieParserReader trieReader, //read data from here
									final TrieParser trieParser,
									final int endId) {       //context is returned with extra bits as needed
				
		DataOutputBlobWriter.tryClearIntBackData(writer, headerCount); 
		
        long postLength = -2;

        int iteration = 0;
        int remainingLen;
        while ((remainingLen=TrieParserReader.parseHasContentLength(trieReader))>0){

        	int headerId = (int)TrieParserReader.parseNext(trieReader, trieParser);
        	
            if (endId == headerId) { 
                return endOfHeadersLogic(writer, errorReporter, 
                		writeIndex, requestContext, trieReader,
						postLength, iteration);
            } else if (-1 == headerId) {            	
            	if (remainingLen>MAX_HEADER) {   
            		//client has sent very bad data.
            		return errorReporter.sendError(400) ? (requestContext | ServerCoordinator.CLOSE_CONNECTION_MASK) : ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
            	}
                //nothing valid was found so this is incomplete.
                return ServerCoordinator.INCOMPLETE_RESPONSE_MASK; 
            }
        	
            if (HTTPHeaderDefaults.CONTENT_LENGTH.ordinal() == headerId) {
            	postLength = TrieParserReader.capturedLongField(trieReader, 0);
            } else if (HTTPHeaderDefaults.TRANSFER_ENCODING.ordinal() == headerId) {
            	postLength = -1;
            } else if (HTTPHeaderDefaults.CONNECTION.ordinal() == headerId) {            	
                requestContext = applyKeepAliveOrCloseToContext(requestContext, trieReader);                
            }
                        
            HeaderUtil.captureRequestedHeader(writer, indexOffsetCount, headerToPositionTable, writeIndex, trieReader, headerId);
            iteration++;
        }
        return ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
	}

	private static int endOfHeadersLogic(DataOutputBlobWriter<HTTPRequestSchema> writer, ErrorReporter errorReporter,
			final boolean writeIndex, int requestContext, final TrieParserReader trieReader, long postLength,
			int iteration) {
		if (iteration==0) {
			//needs more data 
			requestContext = ServerCoordinator.INCOMPLETE_RESPONSE_MASK;                	
		} else {	          
			//logger.trace("end of request found");
			//THIS IS THE ONLY POINT WHERE WE EXIT THIS MTHOD WITH A COMPLETE PARSE OF THE HEADER, 
			//ALL OTHERS MUST RETURN INCOMPLETE
			
			if (DataOutputBlobWriter.lastBackPositionOfIndex(writer)<writer.position()) {
					
				logger.warn("pipes are too small for this many headers, max total header size is "+writer.getPipe().maxVarLen);	
				requestContext = errorReporter.sendError(503) ? (requestContext | ServerCoordinator.CLOSE_CONNECTION_MASK) : ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
			} else if (postLength>0) {
		   		//full length is done here as a single call, the pipe must be large enough to hold the entire payload
		   		
		   		assert(postLength<Integer.MAX_VALUE);
		   		
		   		//read data directly
				final int writePosition = writer.position();  
				
				if (DataOutputBlobWriter.lastBackPositionOfIndex(writer)<(writePosition+postLength)) {
					logger.warn("unable to take large post at this time");	
					requestContext = errorReporter.sendError(503) ? (requestContext | ServerCoordinator.CLOSE_CONNECTION_MASK) : ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
				}
				
				int cpyLen = TrieParserReader.parseCopy(trieReader, postLength, writer);
				if (cpyLen<postLength) {
				   //needs more data 
					requestContext = ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
				} else {	           			
		   			if (writeIndex) {
		   				//logger.info("write the position of the body to {} ",writePosition);
		   				//NOTE: record position of the body in the new way, at beginning of the index
		   				DataOutputBlobWriter.setIntBackData(writer, writePosition, 1);
						
					}
				}
		   	}     
		}
		return requestContext;
	}


	private static int applyKeepAliveOrCloseToContext(int requestContext, TrieParserReader trieReader) {
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
		    case 'p': //Upgrade
		        requestContext |= ServerCoordinator.UPGRADE_MASK;                        
		        break;
		    default:
		       	logger.warn("Fix TrieParser query:   Connection: {}",TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, new StringBuilder()) );             
		    	
		    	//logger.warn("ERROR: {}",TrieParserReader.capturedFieldBytesAsUTF8Debug(trieReader, 0, new StringBuilder()) );
		    	return ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
		    	
		}
		return requestContext;
	}


	private static int keepAliveOrNotContext(int revisionId) {
		int requestContext = 0; //by default this is keep alive, eg zero.
        
        if(
           (HTTPRevisionDefaults.HTTP_0_9.ordinal() == revisionId) ||
           (HTTPRevisionDefaults.HTTP_1_0.ordinal() == revisionId)
          ) {
            requestContext |= ServerCoordinator.CLOSE_CONNECTION_MASK;
        }
		return requestContext;
	}
    
}
