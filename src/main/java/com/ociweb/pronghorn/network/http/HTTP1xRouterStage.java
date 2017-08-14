package com.ociweb.pronghorn.network.http;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.SSLConnection;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
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
    
	private static final int MAX_URL_LENGTH = 4096;
    private static Logger logger = LoggerFactory.getLogger(HTTP1xRouterStage.class);

    public static boolean showHeader = false; //set to true for debug to see headers in console.
    
    
    private TrieParserReader trieReader;
    private long activeChannel;
    
    private ErrorReporter errorReporter = new ErrorReporter() {

			@Override
			public boolean sendError(int errorCode) {				
				return HTTP1xRouterStage.this.sendError(activeChannel, idx, errorCode); 	
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
 
        int h = config.routesCount();
        
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
    	
        if (waitForOutputOn>=0) { //hack test
        	
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
        	
        	int start = inputLengths[idx];
            int messageIdx = accumulateRunningBytes(idx, selectedInput);
            if (messageIdx < 0) {
            	//logger.trace("detected EOF for {}",idx);
            	//accumulate these before shutdown?? also wait for all data to be consuemd.
                isOpen[idx] = false;
                if (inputLengths[idx]<=0) {
                	return -1;
                }
            }
            
            ///TODO: only do this if we had to stop parse looking for data URGENT next.
            if (needsData[idx]) {
	            if (inputLengths[idx]==start) {
	            	//we got no data so move on to the next
	            	return 0;
	            } else {
	            	//logger.info("BBBB "+inputChannels[idx]);
	            	needsData[idx]=false;
	            }
            }            
        }

        long channel   = inputChannels[idx];
        activeChannel = channel;

        if (channel >= 0) {
        	int result = 0;

        	assert(inputLengths[idx]>0) : "length is "+inputLengths[idx];
        	
        	
        	assert(inputBlobPos[idx]+inputLengths[idx] == inputBlobPosLimit[idx]) : "length mismatch";
       	
        	assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));
        	
        	TrieParserReader.parseSetup(trieReader, Pipe.blob(selectedInput), inputBlobPos[idx], inputLengths[idx], Pipe.blobMask(selectedInput));	
        	
        	//final long toParseLength = TrieParserReader.parseHasContentLength(trieReader);
        	
        	boolean debugdata = false;
        	if (debugdata) {
        		assert(validateNextByte(trieReader, idx));
        	}
        	
        	do {
        		assert(inputLengths[idx]>0) : "length is "+inputLengths[idx]; 
	            result = consumeAvail(idx, selectedInput, channel, inputBlobPos[idx], inputLengths[idx]);	           
        	} while (result>0);
        	
        	assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));

        	return result;
        } 

    
        return 0;
    }

    
    private int consumeAvail(final int idx, Pipe<NetPayloadSchema> selectedInput, final long channel, int p, int l) {
        
    	    int totalConsumed = 0;
            final long toParseLength = TrieParserReader.parseHasContentLength(trieReader);
            assert(toParseLength>=0) : "length is "+toParseLength+" and input was "+l;
                        
            if (toParseLength>0) {
	            final int result = parseHTTP(trieReader, channel, idx, selectedInput); 
            	if (SUCCESS == result) {
	            	
	                int newTotalConsumed = (int)(toParseLength - TrieParserReader.parseHasContentLength(trieReader));           
	                int consumed = newTotalConsumed-totalConsumed;
	                
	                if (ServerCoordinator.TEST_RECORDS && 73!=consumed) {
	                	logger.info("expected consume of 73 but found {}",consumed);
	                	logger.info("CONSUMED FORCE EXIT ");
	            		System.exit(-1);
	                }
	                
	                
	                totalConsumed = newTotalConsumed;
	                p += consumed;
	                l -= consumed;
	
	                Pipe.releasePendingAsReadLock(selectedInput, consumed);
	               
//	                if (ServerCoordinator.TEST_RECORDS && l>0) {
//				            	char ch = (char)selectedInput.blobRing[p&selectedInput.blobMask];
//				            	if (ch!='G') {
//					            		logger.info("AFTER FORCE EXIT ON BAD DATA {} should be G previous char was {} remaining len is {} ",ch,(char)selectedInput.blobRing[(p-1)&selectedInput.blobMask],l);
//					            		System.exit(-1);
//				            	}
//	                }
	                assert(l == trieReader.sourceLen);
	                inputBlobPos[idx]=p;
	                boolean debug = false;
	                if (debug) {
	                	assert(trieReader.sourcePos==inputBlobPos[idx]);
	                	assert(validateNextByte(trieReader, idx));
	                }
	                assert(boundsCheck(idx, l));
	                
	                inputLengths[idx] = l;	                
	         	                
	                //the release pending above should keep them in algnment and the ounstanding should match
	                assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));
	                
	                if (l==0) {
	                	inputChannels[idx] = -1;//must clear since we are at a safe release point
	                	                        //next read will start with new postion.
	                		//send release if appropriate
	         
	                		if ((trieReader.sourceLen<=0) && consumedAllOfActiveFragment(selectedInput, p)) { 
	                				                			
	                			assert(0==Pipe.releasePendingByteCount(selectedInput));
								sendRelease(channel, idx);
														
							}

	                	//when we have no more data to consume we must exit this loop and get more.
                		//logger.info("l is zero so return zero "+selectedInput);
                		return 0;//must cause a wait to accumulate more
	                }
	           } else if (NEED_MORE_DATA == result) {
	        	
	        	   needsData[idx]=true;      	   //TRY AGAIN AFTER WE PARSE MORE DATA IN.
	        	   return 0;//this is the only way to pick up more data, eg. exit the outer loop with zero.
	        	   
	           } else {
	        	   //TRY AGAIN AFTER PIPE CLEARS
	        	   waitForOutputOn = (-result);
	        	   return 0;//exit to take a break while pipe is full.
	           }
            }
           
            assert(inputLengths[idx] <= (selectedInput.blobMask+(selectedInput.blobMask>>1))) : "This data should have been parsed but was not understood, check parser and/or sender for corruption.";
            
	        return totalConsumed>0?1:0;
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

    
    //TODO: this appears to be required to make this work with the shared routers however it also seems to break  multiple connections???
    //      because some needed releases are missing??
	private boolean consumedAllOfActiveFragment(Pipe<NetPayloadSchema> selectedInput, int p) {
		//TOOD: must check both not sure why it shows up both ways at different times.
		return (Pipe.blobMask(selectedInput)&Pipe.getWorkingBlobRingTailPosition(selectedInput)	) == (Pipe.blobMask(selectedInput)&p) ||
		       (Pipe.blobMask(selectedInput)&Pipe.getWorkingBlobHeadPosition(selectedInput)	) == (Pipe.blobMask(selectedInput)&p);
		
		
		
	}

    
 private final static int NEED_MORE_DATA = 2;
 private final static int SUCCESS = 1;
 
 

// -1 no room on output pipe (do not call again until the pipe is clear, send back pipe to watch)
//  2 need more data to parse (do not call again until data to parse arrives)
//  1 success
// <=0 for wating on this output pipe to have room (the pipe idx is negative)
 
 
private int parseHTTP(TrieParserReader trieReader, final long channel, final int idx, Pipe<NetPayloadSchema> selectedInput) {    
    
	boolean writeIndex = true; //must be on to ensure we can index to the header locations.

    if (showHeader) {
    	System.out.println("///////////////// ROUTE HEADER "+channel+"///////////////////");
    	TrieParserReader.debugAsUTF8(trieReader, System.out, Math.min(8192, trieReader.sourceLen), false); //shows that we did not get all the data
    	System.out.println("...\n///////////////////////////////////////////");
    }

	int tempLen = trieReader.sourceLen;
	int tempPos = trieReader.sourcePos;
    
	if (tempLen<=0) {
		//logger.info("need more data A");
		return NEED_MORE_DATA;
	}
	
	boolean debugdata = false;
	if (debugdata) {
		assert(validateNextByte(trieReader, idx));
	}
	
	final int verbId = (int)TrieParserReader.parseNext(trieReader, config.verbMap);     //  GET /hello/x?x=3 HTTP/1.1     
    if (verbId<0) {
    		if (tempLen < (config.verbMap.longestKnown()+1) || (trieReader.sourceLen<0)) { //added 1 for the space which must appear after
    			//logger.info("need more data B");
    			return NEED_MORE_DATA;    			
    		} else {
    		
    			//logger.info("start at pos "+tempPos+" for "+channel);
        		
    			//we have bad data we have been sent, there is enough data yet the verb was not found
    			trieReader.sourceLen = tempLen;
    			trieReader.sourcePos = tempPos;
    			
    			boolean debug = true; 
    			if(debug) {
    				StringBuilder builder = new StringBuilder();    			    			
    				TrieParserReader.debugAsUTF8(trieReader, builder, config.verbMap.longestKnown()*2);    			
    				logger.warn("{} looking for verb but found:\n{} at position {} from pipe {} \n\n",channel,builder,tempPos,selectedInput);
    			}
    			
    			trieReader.sourceLen = 0;
    			trieReader.sourcePos = 0;    			
    			
    			badClientError(channel);
    			//logger.info("success");
    			return SUCCESS;
    			    		    			
    		}
    		
    }
   // System.err.println("start at pos "+tempPos+" for "+channel);
    
	tempLen = trieReader.sourceLen;
	tempPos = trieReader.sourcePos;
	final int routeId;
    routeId = (int)TrieParserReader.parseNext(trieReader, config.urlMap);     //  GET /hello/x?x=3 HTTP/1.1 

    if (!catchAll && config.UNMAPPED_ROUTE == routeId) {
    	
		//unsupported route path, send 404 error
		sendError(trieReader, channel, idx, tempLen, tempPos, 404);	
		return SUCCESS;
    }
    
    if (routeId<0) {

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
    Pipe<HTTPRequestSchema> outputPipe = routeId<outputs.length ? outputs[routeId] : outputs[0];
    Pipe.markHead(outputPipe);//holds in case we need to abandon our writes
    
    if (Pipe.hasRoomForWrite(outputPipe) ) {

        final int size =  Pipe.addMsgIdx(outputPipe, HTTPRequestSchema.MSG_RESTREQUEST_300);        // Write 1   1                         
        Pipe.addLongValue(channel, outputPipe); // Channel                        // Write 2   3        
 
        Pipe.addIntValue(sequences[idx], outputPipe); //sequence                    // Write 1   4
        
        //route and verb
        Pipe.addIntValue((routeId << HTTPVerb.BITS) | (verbId & HTTPVerb.MASK), outputPipe);// Verb                           // Write 1   5
        
		DataOutputBlobWriter<HTTPRequestSchema> writer = Pipe.outputStream(outputPipe);
		                                                                                                                          //write 2   7
        DataOutputBlobWriter.openField(writer); //the beginning of the payload always starts with the URL arguments

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

    			//we have bad data we have been sent, there is enough data yet the revision was not found
    			trieReader.sourceLen = tempLen;
    			trieReader.sourcePos = tempPos;
    			
    			StringBuilder builder = new StringBuilder();
    			TrieParserReader.debugAsUTF8(trieReader, builder, config.revisionMap.longestKnown()*4);
    			//logger.warn("{} looking for HTTP revision but found:\n{}\n\n",channel,builder);
    		    			
    			trieReader.sourceLen = 0;
    			trieReader.sourcePos = 0;
    			
    			badClientError(channel);
    			//logger.info("success");
    			return SUCCESS;
    		}
        }
        
		////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////also write the requested headers out to the payload
        /////////////////////////////////////////////////////////////////////////
        int requestContext = parseHeaderFields(routeId, errorReporter, writer, httpRevisionId, false, writeIndex);  // Write 2   10 //if header is presen
       
        
        if (ServerCoordinator.INCOMPLETE_RESPONSE_MASK == requestContext) {  
        	DataOutputBlobWriter.closeLowLevelField(writer);
            //try again later, not complete.
            Pipe.resetHead(outputPipe);
            //logger.info("need more data E");
            
           // config.debugURLMap();
            
            return NEED_MORE_DATA;
        } 
        
   
        ////////TODO: URGENT: re-think this as we still need to add the chunked post payload??
        
    	DataOutputBlobWriter.commitBackData(writer);
    	DataOutputBlobWriter.closeLowLevelField(writer);
        
        
		//not an error we just looked past the end and need more data
	    if (trieReader.sourceLen<0) {
	    	Pipe.resetHead(outputPipe);
	    	//logger.info("need more data F");
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
        return -routeId;
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
		SSLConnection con = coordinator.get(channel);
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
		
		int defaultRev = HTTPRevisionDefaults.HTTP_1_1.ordinal();	//TODO: need a better way to look this up.			
		int defaultContent = HTTPContentTypeDefaults.UNKNOWN.ordinal(); //TODO: need a better way to look this up.
						
		//will close connection as soon as error is returned.
		HTTPUtil.publishError(sequences[idx], errorCode, 
				              errorResponsePipe, channel, 
				              config.httpSpec, defaultRev, defaultContent);
		
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
	if (channel<0) {
		throw new UnsupportedOperationException("channel must exist");
	}
	assert(inputSlabPos[idx]>=0);
	//logger.info("send ack for {}",channel);
	if (!Pipe.hasRoomForWrite(releasePipe)) {
		logger.info("warning, to prevent hang we must write this message, write fewer or make pipe longer.");
		//NOTE must spin lock for room, must write or this system may hang.
		Pipe.spinBlockForRoom(releasePipe, Pipe.sizeOf(releasePipe, ReleaseSchema.MSG_RELEASEWITHSEQ_101));	    		
	}

	Pipe.presumeRoomForWrite(releasePipe);
	
	int s = Pipe.addMsgIdx(releasePipe, ReleaseSchema.MSG_RELEASEWITHSEQ_101);
	Pipe.addLongValue(channel,releasePipe);
	Pipe.addLongValue(inputSlabPos[idx],releasePipe);
	Pipe.addIntValue(sequences[idx], releasePipe); //send current sequence number so others can continue at this count.
	sequencesSent[idx] = sequences[idx];
	
	
	Pipe.confirmLowLevelWrite(releasePipe, s);
	Pipe.publishWrites(releasePipe);
	this.inputSlabPos[idx]=-1;

}

private void badClientError(long channel) {
	SSLConnection con = coordinator.get(channel);
	if (null!=con) {
		con.clearPoolReservation();		
		con.close();
	}
}


private int accumulateRunningBytes(final int idx, Pipe<NetPayloadSchema> selectedInput) {
    
    int messageIdx = Integer.MAX_VALUE;

    boolean debug = false;
    if (debug) {
	    logger.info("{} accumulate data rules {} && ({} || {} || {})", idx,
	    		     Pipe.hasContentToRead(selectedInput), 
	    		     hasNoActiveChannel(idx), 
	    		     hasReachedEndOfStream(selectedInput), 
	    		     hasContinuedData(idx, selectedInput) );
    }

    
    while ( //NOTE has content to read looks at slab position between last read and new head.
            
    	   Pipe.hasContentToRead(selectedInput) //must check first to ensure assert is happy
    	   &&	
           ( hasNoActiveChannel(idx) ||      //if we do not have an active channel
        	  hasContinuedData(idx, selectedInput) ||
              hasReachedEndOfStream(selectedInput)  //if we have reached the end of the stream
           )           
            
          ) {

        
        messageIdx = Pipe.takeMsgIdx(selectedInput);
        
        //logger.info("seen message id of {}"+messageIdx);
        
        if (NetPayloadSchema.MSG_PLAIN_210 == messageIdx) {
            long channel   = Pipe.takeLong(selectedInput);
            long arrivalTime = Pipe.takeLong(selectedInput);
            
            this.inputSlabPos[idx] = Pipe.takeLong(selectedInput);            
          
            int meta       = Pipe.takeRingByteMetaData(selectedInput);
            int length     = Pipe.takeRingByteLen(selectedInput);
            int pos        = Pipe.bytePosition(meta, selectedInput, length);                                            
            
            assert(Pipe.byteBackingArray(meta, selectedInput) == Pipe.blob(selectedInput));            
            assert(length<=selectedInput.maxVarLen);
            
            boolean freshStart = (-1 == inputChannels[idx]);
            
            //logger.info("accumulate length: {} for {} begin: {}",length,channel,freshStart);

            assert(inputBlobPos[idx]<=inputBlobPosLimit[idx]) : "position is out of bounds.";
            
			if (freshStart) {
				
				assert(inputLengths[idx]<=0) : "expected to be 0 or negative but found "+inputLengths[idx];
				
				//assign
                inputChannels[idx]  = channel;
                inputLengths[idx]  = length;
                inputBlobPos[idx]   = pos;
                inputBlobPosLimit[idx]  =pos + length;
                
                //logger.info("added new fresh start data of {}",length);
                
                
                //assert('G'== Pipe.blob(selectedInput)[pos&selectedInput.blobMask]) : "expected a GET";

                assert(inputLengths[idx]<selectedInput.sizeOfBlobRing);
                assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));
            } else {
                //confirm match
                assert(inputChannels[idx] == channel) : "Internal error, mixed channels";
                   
                //grow position
                assert(inputLengths[idx]>0) : "not expected to be 0 or negative but found "+inputLengths[idx];
                inputLengths[idx] += length; 
                inputBlobPosLimit[idx] += length;
                
               // logger.info("adding start data of {} for total of {}",length,inputBlobPosLimit[idx]);
               
                
                assert(inputLengths[idx] < Pipe.blobMask(selectedInput)) : "When we roll up is must always be smaller than ring "+inputLengths[idx]+" is too large for "+Pipe.blobMask(selectedInput);
                                
                //may only read up to safe point where head is       
                assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));
                                
            }

			assert(inputLengths[idx]>=0) : "error negative length not supported";

            //if we do not move this forward we will keep reading the same spot up to the new head
            Pipe.confirmLowLevelRead(selectedInput, Pipe.sizeOf(selectedInput, messageIdx));            
            Pipe.readNextWithoutReleasingReadLock(selectedInput); 
            
            
            if (-1 == inputSlabPos[idx]) {
            	inputSlabPos[idx] = Pipe.getWorkingTailPosition(selectedInput); //working and was tested since this is low level with unrleased block.
            }
            assert(inputSlabPos[idx]!=-1);
        } else {
        	if (NetPayloadSchema.MSG_BEGIN_208 == messageIdx) {
        		
        		assert(hasNoActiveChannel(idx)) : "Can not begin a new connection if one is already in progress.";        		
        		assert(0==Pipe.releasePendingByteCount(selectedInput));
        		
        		//logger.info("accumulate begin");
        		//keep this as the base for our counting of sequence
        		int newSeq = Pipe.takeInt(selectedInput);
        		
//        		if (sequencesSent[idx] != sequences[idx]) {
//        			throw new UnsupportedOperationException("changed value after clear, internal error"); //TODO: need to fix this, getting on TLS calls.
//        		}
        		//System.err.println(newSeq+" vs old seq "+sequences[idx]);
        		
        		sequences[idx] = newSeq;
        		
        		Pipe.confirmLowLevelRead(selectedInput, Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_BEGIN_208));
        		//Pipe.releaseReadLock(selectedInput);
        		Pipe.readNextWithoutReleasingReadLock(selectedInput);
        		//do not return, we will go back arround the while again.     		
        	} else {
	            assert(-1 == messageIdx) : "messageIdx:"+messageIdx;
	            if (-1 != messageIdx) {
	            	throw new UnsupportedOperationException("bad id "+messageIdx+" raw data  \n"+selectedInput);
	            }
	            
	            Pipe.confirmLowLevelRead(selectedInput, Pipe.EOF_SIZE);
	            Pipe.readNextWithoutReleasingReadLock(selectedInput);
	            return messageIdx;//do not loop again just exit now
        	}
        }        
    }
    

 
    return messageIdx;
}





	private boolean hasContinuedData(int idx, Pipe<NetPayloadSchema> selectedInput) {
	    return (Pipe.hasContentToRead(selectedInput) && Pipe.peekLong(selectedInput, 1)==inputChannels[idx]);
	}
	
	
	private boolean hasReachedEndOfStream(Pipe<NetPayloadSchema> selectedInput) {
	    return (Pipe.hasContentToRead(selectedInput) && Pipe.peekInt(selectedInput)<0);
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
    private int parseHeaderFields(final int routeId, ErrorReporter errorReporter,  
    		                      DataOutputBlobWriter<HTTPRequestSchema> writer, 
    		                      int revisionId, boolean debugMode, boolean writeIndex) {
               
    	return parseHeaderFields(writer, errorReporter, 
				                 config.extractionParser(routeId).getIndexCount(), 
				                 config.headerCount(routeId),
				                 config.headerToPositionTable(routeId), 
				                 writeIndex, 
				                 keepAliveOrNotContext(revisionId),
				                 trieReader,
				                 config.headerMap,
				                 config.END_OF_HEADER_ID);

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
        	
            if (HTTPHeaderDefaults.UPGRADE.ordinal() == headerId) {
                //list of protocols to upgrade to 
                
                //IF TEXT  CONTAINS
                //h2c       upgrade to that
                //websocket
                
                //if no match is found must do deeper parse
                
                //clear internal mask bit
            	
            	logger.warn("Upgrade reqeust deteced but not yet implemented.");
            } else if (HTTPHeaderDefaults.CONTENT_LENGTH.ordinal() == headerId) {
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
			
			if (writer.position()+DataOutputBlobWriter.countOfBytesUsedByIndex(writer) > writer.getPipe().maxVarLen) {
				logger.warn("pipes are too small for this many headers, max total header size is "+writer.getPipe().maxVarLen);	
				requestContext = errorReporter.sendError(503) ? (requestContext | ServerCoordinator.CLOSE_CONNECTION_MASK) : ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
			} else if (postLength>0) {
		   		//full length is done here as a single call, the pipe must be large enough to hold the entire payload
		   		
		   		assert(postLength<Integer.MAX_VALUE);
		   		//read data directly
				int writePosition = writer.position();    
				if (writePosition+postLength+DataOutputBlobWriter.countOfBytesUsedByIndex(writer) >writer.getPipe().maxVarLen) {
					logger.warn("unable to take large post at this time");	
					requestContext = errorReporter.sendError(503) ? (requestContext | ServerCoordinator.CLOSE_CONNECTION_MASK) : ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
				}
				
				int cpyLen = TrieParserReader.parseCopy(trieReader, postLength, writer);
				if (cpyLen<postLength) {
				   //needs more data 
					requestContext = ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
				} else {	           			
		   			if (writeIndex) {
						boolean ok = DataOutputBlobWriter.tryWriteIntBackData(writer, writePosition);
						assert(ok) : "the pipe is too small for the payload";
						if (!ok) {
							logger.warn("unable to take large post at this time");
							requestContext = errorReporter.sendError(503) ? (requestContext | ServerCoordinator.CLOSE_CONNECTION_MASK) : ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
						}
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


 

    
}
