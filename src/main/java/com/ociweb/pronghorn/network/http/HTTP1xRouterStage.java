package com.ociweb.pronghorn.network.http;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.BaseConnection;
import com.ociweb.pronghorn.network.ServerConnection;
import com.ociweb.pronghorn.network.ServerConnectionStruct;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.schema.HTTPLogRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.ChannelWriterController;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

/**
 * Main HTTP router. Quickly redirects any incoming traffic to corresponding routes.
 *
 * @param <T>
 * @param <R>
 * @param <V>
 * @param <H>
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class HTTP1xRouterStage<T extends Enum<T> & HTTPContentType,
							   R extends Enum<R> & HTTPRevision,
						       V extends Enum<V> & HTTPVerb,
                               H extends Enum<H> & HTTPHeader
                             > extends PronghornStage {

    //TODO: double check that this all works with ipv6.
    
	private static final int SIZE_OF_BEGIN = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_BEGIN_208);
	private static final int SIZE_OF_PLAIN = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210);
	private static final int MAX_URL_LENGTH = 4096;
    private static Logger logger = LoggerFactory.getLogger(HTTP1xRouterStage.class);

    public static boolean showHeader = false; //set to true for debug to see headers in console.
    
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
    private final Pipe<HTTPLogRequestSchema> log;
    
    private       long[]                   inputChannels;
    private       int[]                    inputBlobPos;
    private       int[]                    inputBlobPosLimit;
    
    private       int[]                    inputLengths;
    private       boolean[]                needsData;
        
    private final Pipe<ReleaseSchema> releasePipe;
    
    private final Pipe<HTTPRequestSchema>[] outputs;
    
    private int   waitForOutputOn = -1;

    private long[] inputSlabPos;
    private int[] sequences;
        
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
    		                               Pipe<HTTPLogRequestSchema> log,
    		                               Pipe<ReleaseSchema> ackStop,
                                           HTTP1xRouterStageConfig<T,R,V,H> config, 
                                           ServerCoordinator coordinator, boolean catchAll) {
        
       return new HTTP1xRouterStage<T,R,V,H>(gm,parallelId,input,outputs, errorResponsePipe, log, ackStop, config, coordinator, catchAll); 
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
		                           Pipe<HTTPLogRequestSchema> log,
		                           Pipe<ReleaseSchema> ackStop,
		                           HTTP1xRouterStageConfig<T,R,V,H> config, 
		                           ServerCoordinator coordinator, boolean catchAll) {
		
		return new HTTP1xRouterStage<T,R,V,H>(gm,parallelId,
				input,outputs, errorResponsePipe, log, 
				ackStop, config, coordinator, catchAll); 
	}

	/**
	 *
	 * @param gm
	 * @param parallelId
	 * @param input _in_ The payload that will be routed.
	 * @param outputs _out_ The HTTP request parsed from the NetPayloadSchema.
	 * @param errorResponsePipe _out If error occurs, it will be written to this pipe.
	 * @param log _out_ Logging output.
	 * @param ackStop _out_ Acknowledgment for ReleaseSchema.
	 * @param config
	 * @param coordinator
	 * @param catchAll
	 */
	public HTTP1xRouterStage(GraphManager gm, 
			int parallelId,
            Pipe<NetPayloadSchema>[] input, Pipe<HTTPRequestSchema>[][] outputs, 
            Pipe<ServerResponseSchema> errorResponsePipe, 
            Pipe<HTTPLogRequestSchema> log,
            Pipe<ReleaseSchema> ackStop,
            HTTP1xRouterStageConfig<T,R,V,H> config, ServerCoordinator coordinator, boolean catchAll) {
		
		this(gm, parallelId, input, join(outputs), errorResponsePipe, log, ackStop, config, coordinator, catchAll);
		
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
			                 Pipe<ServerResponseSchema> errorResponsePipe, 
			                 Pipe<HTTPLogRequestSchema> log,
			                 Pipe<ReleaseSchema> ackStop,
                             HTTP1xRouterStageConfig<T,R,V,H> config, 
                             ServerCoordinator coordinator, 
                             boolean catchAll) {
		
        super(gm,input,join(join(outputs,ackStop,errorResponsePipe),log));
                
        this.parallelId = parallelId;
        
        assert(outputs!=null);
        assert(outputs.length>0);
        
        this.config = config;
        this.inputs = input;
        this.log = log;
        
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
        
        inputSlabPos = new long[inputs.length];
        sequences = new int[inputs.length];
      
        trieReader = new TrieParserReader();//max fields we support capturing.
        
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

		            if (null != log && !Pipe.hasRoomForWrite(log)) {
		            	return;//try later after log pipe is cleared
		            }
		            
		            int result = singlePipe(this, localIdx);
		            
		            if (result>=0) {
		            	didWork+=result;		            	
		            } else {	            		            	
		            	if (--shutdownCount<=0) {
		            		requestShutdown();
		            		return;
		            	}
		            }
		            
		            if (waitForOutputOn<0) {
		            } else {
		            	//abandon until the output pipe is cleared.
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
    private static int singlePipe(HTTP1xRouterStage<?, ?, ?, ?> that, final int idx) {

    	final int start = that.inputLengths[idx];
        if (accumRunningBytes(  that,
        		                     idx, 
				            		 that.inputs[idx],
				            		 that.inputChannels[idx]) >=0) {//message idx   
        	
        	if (!that.needsData[idx]) {
        	} else {        		
        		if (that.inputLengths[idx]!=start) {            			
        			that.needsData[idx]=false;
        		} else {
        			//we got no data so move on to the next
        			return 0;
        		}
        	}
        } else {
        	if (that.inputLengths[idx] <= 0) {
    			//closed so return
    			return -1;
    		} else {
    			//process remaining data if all the data is here
    		}             

    		if (that.needsData[idx]) {
    			//got no data but we need more data
    			logger.warn("Shutting down while doing partial consume of data");
    			return -1;
    		}
        }                
        //the common case is -1 so that is first.
        return ((that.activeChannel = that.inputChannels[idx]) < 0) ? 0 :
	        	(that.parseAvail(idx, that.inputs[idx], that.activeChannel) ? 1 : 0);
    }



	private boolean parseAvail(final int idx, Pipe<NetPayloadSchema> selectedInput, final long channel) {
		
		boolean didWork = false;
			
		boolean webSocketUpgraded = ServerCoordinator.isWebSocketUpgraded(coordinator, channel);
		if (!webSocketUpgraded) {			
			didWork = parseHTTPAvail(this, idx, selectedInput, channel, didWork);
		} else {
			
			final int totalAvail = inputLengths[idx]; 
		    
            final int pipeIdx = ServerCoordinator.getWebSocketPipeIdx(coordinator, channel);
            final Pipe<HTTPRequestSchema> outputPipe = outputs[pipeIdx];
            if (!Pipe.hasRoomForWrite(outputPipe) ) {
             	final int result = -pipeIdx;
             	//TRY AGAIN AFTER PIPE CLEARS
             	this.waitForOutputOn = (-result);
             	return false;//exit to take a break while pipe is full.
            }
			
			final byte[] backing = Pipe.blob(selectedInput);
            final int mask = Pipe.blobMask(selectedInput);
          
            if (totalAvail >= 2) {
            
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
	            if (totalAvail >= (2+length+(msk<<2))) {
	            	/////////////////
	            	//at this point we are committed to doing the write
	            	//we have data and we have room
	            	///////////////////
	            	didWork = true;
	            	return commitWrite(idx, selectedInput, channel, totalAvail,
	            			outputPipe, backing, mask, pos, finOpp,
	            			headerSize, msk, length);
	            } else {       
	            	this.needsData[idx]=true;      	   //TRY AGAIN AFTER WE PARSE MORE DATA IN.
	            	return false;//this is the only way to pick up more data, eg. exit the outer loop with zero.
	            }
            } else {
            	//not even 2 so jump out now
            	
            	this.needsData[idx]=true;      	   //TRY AGAIN AFTER WE PARSE MORE DATA IN.
            	return false;//this is the only way to pick up more data, eg. exit the outer loop with zero.

            }
		}
				
		return didWork;
	}


	private boolean commitWrite(final int idx, Pipe<NetPayloadSchema> selectedInput, final long channel,
			final int totalAvail, final Pipe<HTTPRequestSchema> outputPipe, final byte[] backing, final int mask,
			int pos, int finOpp, int headerSize, int msk, long length) {
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
			Pipe.addIntValue(( ( (            backing[mask & pos++]) << 24) |
			            			( (0xFF & backing[mask & pos++]) << 16) |
			            			( (0xFF & backing[mask & pos++]) << 8) |
			            			  (0xFF & backing[mask & pos++]) ), outputPipe);
			headerSize += 4;
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
		this.needsData[idx]=true;      	   //TRY AGAIN AFTER WE PARSE MORE DATA IN.
		return false;//this is the only way to pick up more data, eg. exit the outer loop with zero.
	}


	private static boolean parseHTTPAvail(HTTP1xRouterStage that, 
			final int idx, Pipe<NetPayloadSchema> selectedInput, 
			final long channel,
			boolean didWork) {
		boolean result;
		//NOTE: we start at the same position until this gets consumed
		TrieParserReader.parseSetup(that.trieReader, 
				                    Pipe.blob(selectedInput), 
				                    that.inputBlobPos[idx], 
				                    that.inputLengths[idx], 
				                    Pipe.blobMask(selectedInput));	

		do {
			assert(that.inputLengths[idx]>=0) : "length is "+that.inputLengths[idx];

			int totalAvail = that.inputLengths[idx];
			final long toParseLength = TrieParserReader.parseHasContentLength(that.trieReader);
			assert(toParseLength>=0) : "length is "+toParseLength+" and input was "+totalAvail;
			            
			if (toParseLength>0 && (null==that.log || Pipe.hasRoomForWrite(that.log))) {

		        long arrivalTime = captureArrivalTime(selectedInput);
		        int seqForLogging = that.sequences[idx];
		        int posForLogging = that.trieReader.sourcePos;
		        
				int state = that.parseHTTP(that.trieReader, channel, idx, arrivalTime);				
				int totalConsumed = (int)(toParseLength - TrieParserReader.parseHasContentLength(that.trieReader));           
				int remainingBytes = that.trieReader.sourceLen;
						
				if (SUCCESS == state) {
									
					
					boolean logTrafficEnabled = null != that.log;
					
					if (logTrafficEnabled) {
						
						//this logs every input at this point
						Pipe<HTTPLogRequestSchema> logOut = that.log;
					
						Pipe.presumeRoomForWrite(logOut);//checked above					
						int size = Pipe.addMsgIdx(logOut, HTTPLogRequestSchema.MSG_REQUEST_1);
						Pipe.addLongValue(arrivalTime, logOut);
						Pipe.addLongValue(channel, logOut);
						Pipe.addIntValue(seqForLogging, logOut);
				
						Pipe.addByteArray(Pipe.blob(selectedInput),
								          posForLogging, 
								          totalConsumed,  
								          that.trieReader.sourceMask, logOut);
						
						Pipe.confirmLowLevelWrite(logOut, size);
						Pipe.publishWrites(logOut);					
						
					}
					
					result = consumeBlock(that, idx, selectedInput, channel,
							              that.inputBlobPos[idx], 
							              totalAvail, totalConsumed, 
							              remainingBytes, arrivalTime);
				 } else if (NEED_MORE_DATA == state) {				
					that.needsData[idx]=true;      	   //TRY AGAIN AFTER WE PARSE MORE DATA IN.
					result = false;   
				 } else {
				    //TRY AGAIN AFTER PIPE CLEARS
				    that.waitForOutputOn = (-state);
				    result = false;
				 }
				
				didWork|=result;
			} else {
			
				assert(that.inputLengths[idx] <= (selectedInput.blobMask+(selectedInput.blobMask>>1))) : "This data should have been parsed but was not understood, check parser and/or sender for corruption.";
				result = false;
			} 
		   	           
		} while (result);
		return didWork;
	}




	private static boolean consumeBlock(HTTP1xRouterStage that, final int idx, Pipe<NetPayloadSchema> selectedInput,
			final long channel, int p, int totalAvail, 
			int totalConsumed, int remainingBytes, long arrivalTime) {
		assert(totalConsumed>0);
				
		p += totalConsumed;
		totalAvail -= totalConsumed;

		Pipe.releasePendingAsReadLock(selectedInput, totalConsumed);

		assert(totalAvail == remainingBytes);
		that.inputBlobPos[idx] = p;

		assert(that.boundsCheck(idx, totalAvail));
		
		that.inputLengths[idx] = totalAvail;	                
		            
		//the release pending above should keep them in algnment and the ounstanding should match
		assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, that.inputBlobPos[idx], that.inputLengths[idx]));
		
		if (totalAvail==0) {
			that.inputChannels[idx] = -1;//must clear since we are at a safe release point
			                        //next read will start with new postion.
				//send release if appropriate
      
				if ((remainingBytes<=0) 
					&& consumedAllOfActiveFragment(selectedInput, p)) { 
			
						assert(0==Pipe.releasePendingByteCount(selectedInput));
						that.sendRelease(channel, idx);
																
				}
		}
		return totalConsumed>0;
	}


	private static long captureArrivalTime(Pipe<NetPayloadSchema> selectedInput) {
		long arrivalTime = -1;
		
		long pos = Pipe.tailPosition(selectedInput);
		long head = Pipe.headPosition(selectedInput);
		
		int id = Pipe.readInt(Pipe.slab(selectedInput), 
			         		  Pipe.slabMask(selectedInput),
			         		  pos);
		
		//skip over messages which do not contain any arival time.
		while (id!=NetPayloadSchema.MSG_PLAIN_210 
		     && id!=NetPayloadSchema.MSG_ENCRYPTED_200 
		     && pos<head) {
			
			pos += Pipe.sizeOf(NetPayloadSchema.instance, id);	
			id = Pipe.readInt(Pipe.slab(selectedInput), 
	         		  Pipe.slabMask(selectedInput),
	         		  pos);
			
		}
		
		
		if (pos<head) {
			arrivalTime = Pipe.readLong(Pipe.slab(selectedInput), 
											 Pipe.slabMask(selectedInput),
											 pos + 3);

		} else {
			logger.info("unable to capture arrival time for {}",id);
		}
		return arrivalTime;
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

		return (Pipe.blobMask(selectedInput)&Pipe.getWorkingBlobTailPosition(selectedInput)	) == (Pipe.blobMask(selectedInput)&p) && 
		       (Pipe.blobMask(selectedInput)&Pipe.getWorkingBlobHeadPosition(selectedInput)	) == (Pipe.blobMask(selectedInput)&p);
		
	}

    
 private final static int NEED_MORE_DATA = 2;
 private final static int SUCCESS = 1;
 
// -1 no room on output pipe (do not call again until the pipe is clear, send back pipe to watch)
//  2 need more data to parse (do not call again until data to parse arrives)
//  1 success
// <=0 for wating on this output pipe to have room (the pipe idx is negative)
 
 
private int parseHTTP(TrieParserReader trieReader, final long channel, final int idx, long arrivalTime) {    

    if (showHeader) {
    	System.out.println("///////////////// ROUTE HEADER "+channel+" pos:"+trieReader.sourcePos+"///////////////////");
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
    			//trieReader.debugAsUTF8(trieReader, System.out);
    			logger.info("bad HTTP data recieved by server, channel will be closed. Bytes abandoned:{} looking for verb at {} ",tempLen, tempPos);
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
    			
    			BaseConnection con = coordinator.connectionForSessionId(channel);
				if (null!=con) {
					con.clearPoolReservation();
				}
    			//logger.info("success");
    			return SUCCESS;
    			    		    			
    		}
    		
    }
   // System.err.println("start at pos "+tempPos+" for "+channel);
    
    final boolean showTheRouteMap = false;
    if (showTheRouteMap) {
    	config.debugURLMap();
    }
    
	tempLen = trieReader.sourceLen;
	tempPos = trieReader.sourcePos;
	final int pathId = (int)TrieParserReader.parseNext(trieReader, config.urlMap);     //  GET /hello/x?x=3 HTTP/1.1 
	//the above URLS always end with a white space to ensure they match the spec.

	//logger.info("selected path: "+pathId);
	
    
    int routeId;
    if (config.UNMAPPED_ROUTE == pathId) {
	    if (!catchAll) {
	    	
			//unsupported route path, send 404 error			
	    	sendError(trieReader, channel, idx, tempLen, tempPos, 404);	
	    	sequences[idx]++;
			return SUCCESS;
	    }
    	routeId = config.UNMAPPED_ROUTE; 
    } else {
    	routeId = config.getRouteIdForPathId(pathId);
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
        DataOutputBlobWriter.openField(writer); //the beginning of the payload always clear all indexes

 
        int structId;
        TrieParser headerMap;
        
        if (config.UNMAPPED_ROUTE != pathId) {
        	
        	structId = config.extractionParser(pathId).structId;
        	assert(config.getStructIdForRouteId(routeId) == structId) : "internal error";
            DataOutputBlobWriter.tryClearIntBackData(writer,config.totalSizeOfIndexes(structId));
          	if (!captureAllArgsFromURL(trieReader, pathId, writer)) {
          		
          		//////////////////////////////////////////////////////////////////
          		//did not pass validation so we return 404 without giving any clues why
          		//////////////////////////////////////////////////////////////////
    			sendError(trieReader, channel, idx, tempLen, tempPos, 404);	
    			trieReader.sourceLen = 0;
    			trieReader.sourcePos = 0;
    			
    			BaseConnection con = coordinator.connectionForSessionId(channel);
				if (null!=con) {
					con.clearPoolReservation();
				}
				
    			return SUCCESS;
          	}
            
          	
        	headerMap = config.headerParserRouteId( routeId );
        	        
        } else {
       	
        	structId = config.UNMAPPED_STRUCT;
            DataOutputBlobWriter.tryClearIntBackData(writer,config.totalSizeOfIndexes(structId));
            TrieParserReader.writeCapturedValuesToDataOutput(trieReader, writer, config.unmappedIndexPos,null);
            
        	headerMap = config.unmappedHeaders;
      
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
    			
    			BaseConnection con = coordinator.connectionForSessionId(channel);
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
        

        ServerConnection serverConnection = coordinator.connectionForSessionId(channel);
      	
        
	//	int countOfAllPreviousFields = extractionParser.getIndexCount()+indexOffsetCount;
		int requestContext = parseHeaderFields(trieReader, pathId, headerMap, writer, serverConnection, 
												httpRevisionId, config,
												errorReporter, arrivalTime);  // Write 2   10 //if header is presen
       
        
        if (ServerCoordinator.INCOMPLETE_RESPONSE_MASK == requestContext) {  
        	DataOutputBlobWriter.closeLowLevelField(writer);
            //try again later, not complete.
            Pipe.resetHead(outputPipe);
            return NEED_MORE_DATA;
        } 
        
    	DataOutputBlobWriter.commitBackData(writer,structId);
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


private boolean captureAllArgsFromURL(TrieParserReader trieReader, final int pathId,
		DataOutputBlobWriter<HTTPRequestSchema> writer) {

	if (TrieParserReader.writeCapturedValuesToDataOutput(trieReader, writer, 
			                                         config.paramIndexArray(pathId),
			                                         config.paramIndexArrayValidator(pathId)
													)) {
		//load any defaults
		config.processDefaults(writer, pathId);
		return true;
	}
	return false;
}


private static int parseHeaderFields(TrieParserReader trieReader, 
		final int pathId, final TrieParser headerMap, 
		DataOutputBlobWriter<HTTPRequestSchema> writer, 
		ServerConnection serverConnection, int httpRevisionId,
		HTTP1xRouterStageConfig<?, ?, ?, ?> config,
		ErrorReporter errorReporter2, long arrivalTime) {
	

	
	if (null == serverConnection) {
		return ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
	}
	ChannelWriterController cwc = serverConnection.connectionDataWriter();
	if (null != cwc) {
		ChannelWriter cw = cwc.beginWrite();
		if (null != cw) {//try again later if this connection is overloaded
			
			int requestContext = keepAliveOrNotContext(httpRevisionId, serverConnection.id);
				
			long postLength = -2;
			
			int iteration = 0;
			int remainingLen;
			while ((remainingLen=TrieParserReader.parseHasContentLength(trieReader))>0){
			
				long headerToken = TrieParserReader.parseNext(trieReader, headerMap);
				
			    if (HTTPSpecification.END_OF_HEADER_ID == headerToken) { 
			    				    	
					if (iteration!=0) {
						int routeId = config.getRouteIdForPathId(pathId);
						
						return endOfHeadersLogic(writer, cwc, cw, 
								serverConnection.scs,
								errorReporter2, requestContext, 
								trieReader, postLength, arrivalTime, routeId);
						
					} else {	          
						//needs more data 
						cwc.abandonWrite();
						return ServerCoordinator.INCOMPLETE_RESPONSE_MASK;                	
					}
			    } else if (-1 == headerToken) {            	
			    	if (remainingLen>MAX_HEADER) {   
			    		//client has sent very bad data.
			    		logger.info("client has sent bad data connection {} was closed",serverConnection.id);
			    		return errorReporter2.sendError(400) ? (requestContext | ServerCoordinator.CLOSE_CONNECTION_MASK) : ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
			    	}
			        //nothing valid was found so this is incomplete.
			    	cwc.abandonWrite();
			        return ServerCoordinator.INCOMPLETE_RESPONSE_MASK; 
			    }
				
			    if (HTTPSpecification.UNKNOWN_HEADER_ID != headerToken) {	    		
				    HTTPHeader header = config.getAssociatedObject(headerToken);
				    int writePosition = writer.position();
				    
				    if (null!=header) {
				    	if (serverConnection.scs.isEchoHeader(header) ) {
				    		System.err.println("saved header "+header);
				    		//write this to be echoed when responding.
				    		DataOutputBlobWriter.setIntBackData((DataOutputBlobWriter<?>)cw, 
				    				cw.position(), StructRegistry.FIELD_MASK & (int)headerToken);											    		 
				    		TrieParserReader.writeCapturedValuesToDataOutput(trieReader, (DataOutputBlobWriter<?>)cw);
				    	}
				    	
					    if (HTTPHeaderDefaults.CONTENT_LENGTH.ordinal() == header.ordinal()) {
					    	assert(Arrays.equals(HTTPHeaderDefaults.CONTENT_LENGTH.rootBytes(),header.rootBytes())) : "Custom enums must share same ordinal positions, CONTENT_LENGTH does not match";
				
					    	postLength = TrieParserReader.capturedLongField(trieReader, 0);
					    } else if (HTTPHeaderDefaults.TRANSFER_ENCODING.ordinal() == header.ordinal()) {
					    	assert(Arrays.equals(HTTPHeaderDefaults.TRANSFER_ENCODING.rootBytes(),header.rootBytes())) : "Custom enums must share same ordinal positions, TRANSFER_ENCODING does not match";
				
					    	postLength = -1;
					    } else if (HTTPHeaderDefaults.CONNECTION.ordinal() == header.ordinal()) {            	
					    	assert(Arrays.equals(HTTPHeaderDefaults.CONNECTION.rootBytes(),header.rootBytes())) : "Custom enums must share same ordinal positions, CONNECTION does not match";
					    	
					    	requestContext = applyKeepAliveOrCloseToContext(requestContext, trieReader, serverConnection.id);                
					    }			                
		
					    TrieParserReader.writeCapturedValuesToDataOutput(trieReader, writer);
					    DataOutputBlobWriter.setIntBackData(writer, writePosition, StructRegistry.FIELD_MASK & (int)headerToken);
			
				    }
			    } else {
			    	//assert that important headers are not skipped..
			    	assert(confirmCoreHeadersSupported(trieReader));	    	
			    }
			    iteration++;
			}
			cwc.abandonWrite();
		}
	}
	return ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
}

private static boolean confirmCoreHeadersSupported(TrieParserReader trieReader) {
	StringBuilder headerName = new StringBuilder();					
	TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, headerName); //in TRIE if we have any exact matches that run short must no pick anything.
	headerName.append(": ");
	String header = headerName.toString();
			
	if (   HTTPHeaderDefaults.CONTENT_LENGTH.writingRoot().equals(header) 
		|| HTTPHeaderDefaults.CONNECTION.writingRoot().equals(header) 
		|| HTTPHeaderDefaults.TRANSFER_ENCODING.writingRoot().equals(header) 
			) {
		logger.warn("Did not recognize header {}", header);
		return false;
	}
	
	return true;
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
		BaseConnection con = coordinator.connectionForSessionId(channel);
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

	Pipe.confirmLowLevelWrite(releasePipe, s);
	Pipe.publishWrites(releasePipe);
	this.inputSlabPos[idx]=-1;

}

private void badClientError(long channel) {
	BaseConnection con = coordinator.connectionForSessionId(channel);
	if (null!=con) {
		con.clearPoolReservation();		
		con.close();
	}
}


private static int accumRunningBytes(
						 HTTP1xRouterStage<?, ?, ?, ?> that,
						 final int idx, 
		                 Pipe<NetPayloadSchema> selectedInput,
		                 long inChnl) {
	//    boolean debug = true;
	//    if (debug) {
	//		logger.info("{} accumulate data rules {} && ({} || {} || {})", idx,
	//	    		     Pipe.hasContentToRead(selectedInput), 
	//	    		     hasNoActiveChannel(inChnl), 
	//	    		     hasReachedEndOfStream(selectedInput), 
	//	    		     hasContinuedData(selectedInput, inChnl) );
	//    }
	    int messageIdx = Integer.MAX_VALUE;
	
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
	            inChnl = processPlain(that, idx, selectedInput, inChnl);
	        } else {
	        	if (NetPayloadSchema.MSG_BEGIN_208 == messageIdx) {        		
	        		processBegin(that, idx, selectedInput);        		
	        	} else {
		            return processShutdown(selectedInput, messageIdx);
	        	}
	        }        
	    }
	 
	    return messageIdx;
}


private static long processPlain(
		HTTP1xRouterStage that,
		final int idx, Pipe<NetPayloadSchema> selectedInput, long inChnl) {
	final long channel = Pipe.takeLong(selectedInput);
	
	//instead of using arrival time here, we pull it just before release after each 
	//block gets parsed, this provides the accurate time of arrival.
	final long arrivalTime = Pipe.takeLong(selectedInput);
	
	long slabPos;
	that.inputSlabPos[idx] = slabPos = Pipe.takeLong(selectedInput);            
        
	final int meta       = Pipe.takeByteArrayMetaData(selectedInput);
	final int length     = Pipe.takeByteArrayLength(selectedInput);
	final int pos        = Pipe.bytePosition(meta, selectedInput, length);                                            
	
	assert(Pipe.byteBackingArray(meta, selectedInput) == Pipe.blob(selectedInput));            
	assert(length<=selectedInput.maxVarLen);
	assert(that.inputBlobPos[idx]<=that.inputBlobPosLimit[idx]) : "position is out of bounds.";
	
	if (-1 != inChnl) {
		that.plainMatch(idx, selectedInput, channel, length);
	} else {
		inChnl = that.plainFreshStart(idx, selectedInput, channel, length, pos);		
	}

	assert(that.inputLengths[idx]>=0) : "error negative length not supported";

	//if we do not move this forward we will keep reading the same spot up to the new head
	Pipe.confirmLowLevelRead(selectedInput, SIZE_OF_PLAIN);            
	Pipe.readNextWithoutReleasingReadLock(selectedInput);

	if (-1 == slabPos) {
		that.inputSlabPos[idx] = Pipe.getWorkingTailPosition(selectedInput); //working and was tested since this is low level with unrleased block.
	}
	assert(that.inputSlabPos[idx]!=-1);
	return inChnl;
}


private void plainMatch(final int idx, Pipe<NetPayloadSchema> selectedInput,
		                long channel, int length) {
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


	private static int processShutdown(Pipe<NetPayloadSchema> selectedInput, int messageIdx) {
		assert(-1 == messageIdx) : "messageIdx:"+messageIdx;
		if (-1 != messageIdx) {
			throw new UnsupportedOperationException("bad id "+messageIdx+" raw data  \n"+selectedInput);
		}
		
		Pipe.confirmLowLevelRead(selectedInput, Pipe.EOF_SIZE);
		Pipe.readNextWithoutReleasingReadLock(selectedInput);
		return messageIdx;//do not loop again just exit now
	}


	private static void processBegin(
			HTTP1xRouterStage< ?, ?, ?, ?> that, 
			final int idx, Pipe<NetPayloadSchema> selectedInput) {
		assert(hasNoActiveChannel(that.inputChannels[idx])) : "Can not begin a new connection if one is already in progress.";        		
		assert(0==Pipe.releasePendingByteCount(selectedInput));
			     
		     //  	  ServerCoordinator.inServerCount.incrementAndGet();
	       // 	  ServerCoordinator.start = System.nanoTime();
	     	  
		//logger.info("accumulate begin");
		//keep this as the base for our counting of sequence
		that.sequences[idx] = Pipe.takeInt(selectedInput);
		
		Pipe.confirmLowLevelRead(selectedInput, SIZE_OF_BEGIN);
		//Pipe.releaseReadLock(selectedInput);
		Pipe.readNextWithoutReleasingReadLock(selectedInput);
		//do not return, we will go back arround the while again. 
	}


    // Warning Pipe.hasContentToRead(selectedInput) must be called first.
	private static boolean hasNoActiveChannel(long inChnl) {
		return -1 == inChnl;
	}

	// Warning Pipe.hasContentToRead(selectedInput) must be called first.
	private static boolean hasContinuedData(Pipe<NetPayloadSchema> selectedInput, long inChnl) {
		return Pipe.peekLong(selectedInput, 1)==inChnl;
	}
	
	// Warning Pipe.hasContentToRead(selectedInput) must be called first.
	private static boolean hasReachedEndOfStream(Pipe<NetPayloadSchema> selectedInput) {
	    return Pipe.peekInt(selectedInput)<0;
	}
	
    private static int endOfHeadersLogic(DataOutputBlobWriter<HTTPRequestSchema> writer,
    		ChannelWriterController cwc,
    		ChannelWriter cw,
    		ServerConnectionStruct scs, ErrorReporter errorReporter,
			int requestContext, final TrieParserReader trieReader,
			long postLength, long arrivalTime, int routeId) {
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
				cwc.abandonWrite();
				return ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
			} else {	
				
				//NOTE: payload index is always found at position 0 since it is the first field.
				DataOutputBlobWriter.setIntBackData(writer, writePosition, 0);

			}
		}
				
		cw.structured().writeInt(routeId, scs.routeIdFieldId);
		cw.structured().writeLong(System.nanoTime(), scs.businessStartTime);
		cw.structured().writeLong(arrivalTime, scs.arrivalTimeFieldId);
		cw.structured().writeInt(requestContext, scs.contextFieldId); 
				
		cwc.commitWrite();
		return requestContext;
	}


	private static int applyKeepAliveOrCloseToContext(int requestContext, 
			                                          TrieParserReader trieReader,
			                                          long connectionId) {
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
		    	requestContext |= ServerCoordinator.CLOSE_CONNECTION_MASK; 
		    	//System.err.println("close detected in header so connection "+connectionId+" was closed");
		        break;
		    case 'e': //keep-alive
		        requestContext &= (~ServerCoordinator.CLOSE_CONNECTION_MASK);
		        assert(0==(requestContext&ServerCoordinator.CLOSE_CONNECTION_MASK));
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


	private static int keepAliveOrNotContext(int revisionId, long id) {
		int requestContext = 0; //by default this is keep alive, eg zero.
        
        if(
           (HTTPRevisionDefaults.HTTP_0_9.ordinal() == revisionId) ||
           (HTTPRevisionDefaults.HTTP_1_0.ordinal() == revisionId)
          ) {
            requestContext |= ServerCoordinator.CLOSE_CONNECTION_MASK;
            System.err.println("due to old HTTP in use the connection was closed for "+id);
        }
		return requestContext;
	}
    
}
