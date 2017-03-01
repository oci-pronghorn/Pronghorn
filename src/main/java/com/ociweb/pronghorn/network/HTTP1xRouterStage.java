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
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.Pipe.PaddedLong;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTP1xRouterStage<T extends Enum<T> & HTTPContentType,
							   R extends Enum<R> & HTTPRevision,
						       V extends Enum<V> & HTTPVerb,
                               H extends Enum<H> & HTTPHeaderKey
                             > extends PronghornStage {

  //TODO: double check that this all works iwth ipv6.
    
	private static final int MAX_URL_LENGTH = 4096;
    private static Logger logger = LoggerFactory.getLogger(HTTP1xRouterStage.class);

        
    private TrieParserReader trieReader;
    
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

    private DataOutputBlobWriter<HTTPRequestSchema>[] blobWriter;
 
    private byte[][] headerOffsets;
    private int[][] headerBlankBases;
    private long[] inputSlabPos;
    private int[] sequences;
    private int[] sequencesSent;
    
    
    private static int MAX_HEADER = 1<<15;
    

        
    private int headerIdUpgrade    = HTTPHeaderKeyDefaults.UPGRADE.ordinal();
    private int headerIdConnection = HTTPHeaderKeyDefaults.CONNECTION.ordinal();
    private int headerIdContentLength = HTTPHeaderKeyDefaults.CONTENT_LENGTH.ordinal();

  
    private int[] inputCounts;
    
    private int totalShortestRequest;
    private int shutdownCount;
    
    private int idx;
    private final HTTP1xRouterStageConfig<T,R,V,H> config;
    

	private int groupId;
    
    //read all messages and they must have the same channelID
    //total all into one master DataInputReader
       
    
    public static <	T extends Enum<T> & HTTPContentType,
					R extends Enum<R> & HTTPRevision,
					V extends Enum<V> & HTTPVerb,
					H extends Enum<H> & HTTPHeaderKey> 
    HTTP1xRouterStage<T,R,V,H> newInstance(GraphManager gm, 
    		                               Pipe<NetPayloadSchema>[] input, Pipe<HTTPRequestSchema>[][] outputs, Pipe<ReleaseSchema> ackStop,
                                           HTTP1xRouterStageConfig<T,R,V,H> config) {
        
       return new HTTP1xRouterStage<T,R,V,H>(gm,input,outputs, ackStop, config); 
    }
    
    public static <	T extends Enum<T> & HTTPContentType,
	R extends Enum<R> & HTTPRevision,
	V extends Enum<V> & HTTPVerb,
	H extends Enum<H> & HTTPHeaderKey> 
		HTTP1xRouterStage<T,R,V,H> newInstance(GraphManager gm, 
		                           Pipe<NetPayloadSchema>[] input, Pipe<HTTPRequestSchema>[] outputs, Pipe<ReleaseSchema> ackStop,
		                           HTTP1xRouterStageConfig<T,R,V,H> config) {
		
		return new HTTP1xRouterStage<T,R,V,H>(gm,input,outputs, ackStop, config); 
	}

	public HTTP1xRouterStage(GraphManager gm, 
            Pipe<NetPayloadSchema>[] input, Pipe<HTTPRequestSchema>[][] outputs, Pipe<ReleaseSchema> ackStop,
            HTTP1xRouterStageConfig<T,R,V,H> config) {
		
		this(gm, input, join(outputs), ackStop, config);
		
	}
	public HTTP1xRouterStage(GraphManager gm, 
			                 Pipe<NetPayloadSchema>[] input, Pipe<HTTPRequestSchema>[] outputs, Pipe<ReleaseSchema> ackStop,
                             HTTP1xRouterStageConfig<T,R,V,H> config) {
		
        super(gm,input,join(outputs,ackStop));
        this.config = config;
        this.inputs = input;
        this.releasePipe = ackStop;        
        this.outputs = outputs;

        this.shutdownCount = inputs.length;

        		
        supportsBatchedPublish = false;
        supportsBatchedRelease = false;
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
        headerOffsets = new byte[h][];
        headerBlankBases = new int[h][];
        
        while (--h>=0) { 
            byte[] offsets = new byte[config.httpSpec.headerCount+1];
            byte runningOffset = 0;
            
            long mask = config.headerMask(h);
            int fieldCount = 0;
            for(int ordinalValue = 0; ordinalValue<=config.httpSpec.headerCount; ordinalValue++) {
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
        

        trieReader = new TrieParserReader(16);//max fields we support capturing.
        
        int w = outputs.length;
        blobWriter = new DataOutputBlobWriter[w];
        while (--w>=0) {
            blobWriter[w] = new DataOutputBlobWriter<HTTPRequestSchema>(outputs[w]);
        }
        
        int x; 
        
        totalShortestRequest = 0;//count bytes for the shortest known request, this opmization helps prevent parse attempts when its clear that there is not enough data.
        int localShortest;
        

        
        
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
    		logger.info("error, forced shutdown of router while its still waiting for {}", shutdownCount);
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

        	int start = inputLengths[idx];
            int messageIdx = accumulateRunningBytes(idx, selectedInput);
            if (messageIdx < 0) {
            	//logger.info("detected EOF for {}",idx);
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
	            	needsData[idx]=false;
	            }
            }
            
        }        

        
        //TODO: if we did not accumulate anything and the parser has done what it can then we should NOT attempt parse again for this IDX
        //   if we blocked on output do it again? check for blocked output earlier to avoid query parse?
        
        
        long channel   = inputChannels[idx];
        
//        if (nextTime<System.currentTimeMillis()) {
//        	logger.info("setup parse for channel {} {} {} ",channel, isOpen[idx], selectedInput);
//        	nextTime = System.currentTimeMillis()+1_000;
//        }
        
        if (channel >= 0) {
        	int result = 0;

        	assert(inputLengths[idx]>0) : "length is "+inputLengths[idx]; 
       	
        	assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));
        	
        	TrieParserReader.parseSetup(trieReader, Pipe.blob(selectedInput), inputBlobPos[idx], inputLengths[idx], Pipe.blobMask(selectedInput));	   
        	assert(validateNextByte(trieReader, idx));
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
	                assert(trieReader.sourcePos==inputBlobPos[idx]);
	                assert(validateNextByte(trieReader, idx));
	                
	                assert(boundsCheck(idx, l));
	                
	                inputLengths[idx]=l;	                
	                
	                //TODO: add checks to ensure that the tail and head only move foreward.
	                
	                assert(trieReader.sourceLen == Pipe.releasePendingByteCount(selectedInput));
	                
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
	           } else if (NEED_MORE_DATA == result){
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
		       (Pipe.blobMask(selectedInput)&Pipe.getBlobWorkingHeadPosition(selectedInput)	) == (Pipe.blobMask(selectedInput)&p);
		
		
		
	}

    
 private final static int NEED_MORE_DATA = 2;
 private final static int SUCCESS = 1;
 
 

// -1 no room on output pipe (do not call again until the pipe is clear, send back pipe to watch)
//  2 need more data to parse (do not call again until data to parse arrives)
//  1 success
// <=0 for wating on this output pipe to have room (the pipe idx is negative)
 
private int parseHTTP(TrieParserReader trieReader, long channel, final int idx, Pipe<NetPayloadSchema> selectedInput) {    
    
//	boolean showHeader = true;
//    if (showHeader) {
//    	System.out.println("///////////////// ROUTE HEADER "+channel+"///////////////////");
//    	TrieParserReader.debugAsUTF8(trieReader, System.out, Math.min(110, trieReader.sourceLen), false); //shows that we did not get all the data
////    	System.out.println("prev 10 data\n");
////    	trieReader.sourcePos -= 10;
////    	TrieParserReader.debugAsUTF8(trieReader, System.out, Math.min(110, trieReader.sourceLen), false);
////    	trieReader.sourcePos +=10;
//    	System.out.println("...\n///////////////////////////////////////////");
//    }

	int tempLen = trieReader.sourceLen;
	int tempPos = trieReader.sourcePos;
    
	if (tempLen<=0) {
		return NEED_MORE_DATA;
	}
	
	assert(validateNextByte(trieReader, idx));

//	char ch = (char)selectedInput.blobRing[trieReader.sourcePos&selectedInput.blobMask];
//	if (ch!='G') {
//		logger.info("AT TOP FORCE EXIT ON BAD DATA {} SHOULD BE G PREVIOUS VALUE WAS {} len {} ",ch, (char)selectedInput.blobRing[(trieReader.sourcePos-1)&selectedInput.blobMask], trieReader.sourceLen);
//		System.exit(-1);
//	}
	
	
	final int verbId = (int)TrieParserReader.parseNext(trieReader, config.verbMap);     //  GET /hello/x?x=3 HTTP/1.1     
    if (verbId<0) {
    		if (tempLen < (config.verbMap.longestKnown()+1)) { //added 1 for the space which must appear after
    //			logger.info("A. waiting on verb for {}",channel);
    			return NEED_MORE_DATA;    			
    		} else {
    			if (trieReader.sourceLen<0) {
    //				logger.info("B. waiting on verb for {}",channel);
    		    	return NEED_MORE_DATA;
    		    }
    			//we have bad data we have been sent, there is enough data yet the verb was not found
    			trieReader.sourceLen = tempLen;
    			trieReader.sourcePos = tempPos;
    			
    			StringBuilder builder = new StringBuilder();
    			    			
    			TrieParserReader.debugAsUTF8(trieReader, builder, config.verbMap.longestKnown()*2);    			
    			logger.warn("{} looking for verb but found:\n{} at position {} from pipe {} \n\n",channel,builder,tempPos,selectedInput);
    		    			
    			badClientError(channel);
    			    		    			
    		}
    }
    
	tempLen = trieReader.sourceLen;
	tempPos = trieReader.sourcePos;
    final int routeId = (int)TrieParserReader.parseNext(trieReader, config.urlMap);     //  GET /hello/x?x=3 HTTP/1.1 
    if (routeId<0) {
    	if (tempLen < MAX_URL_LENGTH) {
//   		logger.info("A. waiting on route for {}",channel);
			return NEED_MORE_DATA;    			
		} else {
		    if (trieReader.sourceLen<0) {
//		    	logger.info("B. waiting on route for {}",channel);
		    	return NEED_MORE_DATA;
		    } 
			//we have bad data we have been sent, there is enough data yet the verb was not found
			trieReader.sourceLen = tempLen;
			trieReader.sourcePos = tempPos;

			StringBuilder builder = new StringBuilder();
			TrieParserReader.debugAsUTF8(trieReader, builder, MAX_URL_LENGTH);			
			logger.warn("{} looking for URL to route but found:\n\"{}\"\n\n",channel,builder);
		    			
			badClientError(channel);
		}
    }
 
    //if thie above code went past the end OR if there is not enough room for an empty header  line maker then return
    if (trieReader.sourceLen<2) {
   // 	logger.info("D. waiting on route for {}",channel);
    	return NEED_MORE_DATA;
    }    	
    
    Pipe<HTTPRequestSchema> outputPipe = outputs[routeId];
    Pipe.markHead(outputPipe);//holds in case we need to abandon our writes
    
    if (Pipe.hasRoomForWrite(outputPipe) ) {
        
        final int size =  Pipe.addMsgIdx(outputPipe, HTTPRequestSchema.MSG_RESTREQUEST_300);        // Write 1   1                         
        Pipe.addLongValue(channel, outputPipe); // Channel                        // Write 2   3        
        
       // System.out.println("wrote ever increasing squence from router of "+sequenceNo);//TODO: should this be per connection instead!!
        
        Pipe.addIntValue(sequences[idx], outputPipe); //sequence                    // Write 1   4
        Pipe.addIntValue(verbId, outputPipe);   // Verb                           // Write 1   5
        
        writeURLParamsToField(trieReader, blobWriter[routeId]);                          //write 2   7
       
        
    	tempLen = trieReader.sourceLen;
    	tempPos = trieReader.sourcePos;
        int httpRevisionId = (int)TrieParserReader.parseNext(trieReader, config.revisionMap);  //  GET /hello/x?x=3 HTTP/1.1 
        
        if (httpRevisionId<0) { 
        	if (tempLen < (config.revisionMap.longestKnown()+1)) { //added 1 for the space which must appear after
        		Pipe.resetHead(outputPipe);
   //    		logger.info("A. waiting on revision for {}",channel);
    			return NEED_MORE_DATA;    			
    		} else {
    			//not an error we just looked past the end
    		    if (trieReader.sourceLen<0) {
    //		    	    logger.info("B. waiting on revision for {}",channel);
    		    	    Pipe.resetHead(outputPipe);
    			    	return NEED_MORE_DATA;
    			}    
    			//we have bad data we have been sent, there is enough data yet the verb was not found
    			trieReader.sourceLen = tempLen;
    			trieReader.sourcePos = tempPos;
    			
    			StringBuilder builder = new StringBuilder();
    			TrieParserReader.debugAsUTF8(trieReader, builder, config.revisionMap.longestKnown()*2);
    			logger.warn("{} looking for HTTP revision but found:\n{}\n\n",channel,builder);
    		    			
    			badClientError(channel);		
    		}

        }
        
        Pipe.addIntValue(httpRevisionId, outputPipe); // Revision Id          // Write 1   8

        int requestContext = parseHeaderFields(routeId, outputPipe, httpRevisionId, false);  // Write 2   10 //if header is presen
        if (ServerCoordinator.INCOMPLETE_RESPONSE_MASK == requestContext) {   
            //try again later, not complete.
            Pipe.resetHead(outputPipe);
   //         logger.info("A. waiting on headers for {}",channel);
            return NEED_MORE_DATA;
        }        
		//not an error we just looked past the end and need more data
	    if (trieReader.sourceLen<0) {
	//    	logger.info("B. waiting on headers for {}",channel);
	    	    Pipe.resetHead(outputPipe);
		    	return NEED_MORE_DATA;
		} 
        Pipe.addIntValue(requestContext, outputPipe); // request context     // Write 1   11
        
        int consumed = Pipe.publishWrites(outputPipe);                        // Write 1   12
        assert(consumed>=0);        
        Pipe.confirmLowLevelWrite(outputPipe, size); 
        sequences[idx]++; //increment the sequence since we have now published the route.

        assert(validateNextByte(trieReader, idx));
        //logger.info("normal finish");
        
        
    } else {
 //   	logger.info("No room, waiting for {} {}",channel, outputPipe);
        //no room try again later
        return -routeId;
    }
    
   inputCounts[idx]++; 
   assert(validateNextByte(trieReader, idx));
   return SUCCESS;
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

	if (Pipe.hasRoomForWrite(releasePipe)) {	   
		int s = Pipe.addMsgIdx(releasePipe, ReleaseSchema.MSG_RELEASEWITHSEQ_101);
		Pipe.addLongValue(channel,releasePipe);
		Pipe.addLongValue(inputSlabPos[idx],releasePipe);
		Pipe.addIntValue(sequences[idx], releasePipe); //send current sequence number so others can continue at this count.
		sequencesSent[idx] = sequences[idx];
		
		
		Pipe.confirmLowLevelWrite(releasePipe, s);
		Pipe.publishWrites(releasePipe);
		this.inputSlabPos[idx]=-1;
	} else {
		logger.error("BBBBBBBBBBBBBBBBB server no room for ack of {} {}",channel,releasePipe);
	}
}

private void badClientError(long channel) {
//	SSLConnection con = coordinator.get(channel, groupId);
//	if (null!=con) {
//		con.close();
//	}
	//TODO: drop connection we have bad player
	new UnsupportedOperationException("TODO: add drop connection support").printStackTrace();
	logger.error("not implemented exiting now");
	System.exit(-1);
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



private int accumulateRunningBytes(final int idx, Pipe<NetPayloadSchema> selectedInput) {
    
    int messageIdx = Integer.MAX_VALUE;

//	    logger.info("{} accumulate data rules {} && ({} || {} || {})", idx,
//	    		     Pipe.hasContentToRead(selectedInput), 
//	    		     hasNoActiveChannel(idx), 
//	    		     hasReachedEndOfStream(selectedInput), 
//	    		     hasContinuedData(idx, selectedInput) );
	    

    
    while ( //NOTE has content to read looks at slab position between last read and new head.
            
    	   Pipe.hasContentToRead(selectedInput) //must check first to ensure assert is happy
    	   &&	
           ( hasNoActiveChannel(idx) ||      //if we do not have an active channel
        	  hasContinuedData(idx, selectedInput) ||
              hasReachedEndOfStream(selectedInput)  //if we have reached the end of the stream
           )           
            
          ) {

        
        messageIdx = Pipe.takeMsgIdx(selectedInput);
        
        if (NetPayloadSchema.MSG_PLAIN_210 == messageIdx) {
            long channel   = Pipe.takeLong(selectedInput);
            long arrivalTime = Pipe.takeLong(selectedInput);
            
            this.inputSlabPos[idx] = Pipe.takeLong(selectedInput);            
          
            int meta       = Pipe.takeRingByteMetaData(selectedInput);
            int length     = Pipe.takeRingByteLen(selectedInput);
            int pos        = Pipe.bytePosition(meta, selectedInput, length);                                            
            
            assert(Pipe.byteBackingArray(meta, selectedInput) == Pipe.blob(selectedInput));            
            assert(length>0) : "value:"+length;
            assert(length<=selectedInput.maxAvgVarLen);
            
            boolean freshStart = (-1 == inputChannels[idx]);
            
        //    System.out.println("accumulate length: "+length+" for "+channel+" begin:"+freshStart);

            assert(inputBlobPos[idx]<=inputBlobPosLimit[idx]) : "position is out of bounds.";
            
			if (freshStart) {
				
				assert(inputLengths[idx]<=0) : "expected to be 0 or negative but found "+inputLengths[idx];
				
				//assign
                inputChannels[idx]  = channel;
                inputLengths[idx]   = length;
                inputBlobPos[idx]   = pos;
                inputBlobPosLimit[idx]=pos+length;
                
                assert('G'== Pipe.blob(selectedInput)[pos&selectedInput.blobMask]) : "expected a GET";

                assert(inputLengths[idx]<selectedInput.sizeOfBlobRing);
                assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));
            } else {
                //confirm match
                assert(inputChannels[idx] == channel) : "Internal error, mixed channels";
                   
                //grow position
                assert(inputLengths[idx]>0) : "not expected to be 0 or negative but found "+inputLengths[idx];
                inputLengths[idx] += length; 
                inputBlobPosLimit[idx]+=length;

                assert(inputLengths[idx] < Pipe.blobMask(selectedInput)) : "When we roll up is must always be smaller than ring "+inputLengths[idx]+" is too large for "+Pipe.blobMask(selectedInput);
                                
                //may only read up to safe point where head is       
                assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));
                                
            }

            if (inputLengths[idx]<0) {
            	
            	new Exception("error negative length not supported"+inputLengths[idx]).printStackTrace();
            	
            }
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
        		
        		//keep this as the base for our counting of sequence
        		int newSeq = Pipe.takeInt(selectedInput);
        		
        		if (sequencesSent[idx] != sequences[idx]) {
        			throw new UnsupportedOperationException("changed value after clear, internal error");
        		}
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
        long headerMask = config.headerMask(routeId);
        
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
        int remainingLen;
        while ((remainingLen=TrieParserReader.parseHasContentLength(trieReader))>0){
        
        	boolean watch = false;
        	int alen=trieReader.sourceLen;
        	int apos=trieReader.sourcePos;
        	if (12==trieReader.sourceLen) {
        		watch = true;        		
        	}
        	int headerId = (int)TrieParserReader.parseNext(trieReader, config.headerMap);
        	       	
        	
            if (config.END_OF_HEADER_ID == headerId) {
                if (iteration==0) {
                	//needs more data 
                	return ServerCoordinator.INCOMPLETE_RESPONSE_MASK; 
                	//      throw new RuntimeException("should not have found end of header");
                } else {
                     assignMissingHeadersNull(staticRequestPipe, headerMask, basePos, offsets);
                }             
               // logger.info("end of request found");
                //THIS IS THE ONLY POINT WHERE WE EXIT THIS MTHOD WITH A COMPLETE PARSE OF THE HEADER, ALL OTHERS MUST RETURN INCOMPLETE
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
            	
            	logger.warn("Upgrade reqeust deteced but not yet implemented.");
            } else if (headerIdContentLength == headerId) {
            	
            	logger.warn("post or payload not yet implemented, content length detected");
            	
              
            } else if (headerIdConnection == headerId) {
            	if (watch) {

            		trieReader.sourceLen = alen;
            		trieReader.sourcePos = apos;
            		int temp = (int)TrieParserReader.parseNext(trieReader, config.headerMap);
               	 
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
        return ServerCoordinator.INCOMPLETE_RESPONSE_MASK;

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
