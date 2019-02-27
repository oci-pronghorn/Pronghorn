package com.ociweb.pronghorn.network.http;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.BaseConnection;
import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ServerConnection;
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
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.PipeWorkWatcher;
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
    
	private static final long lingerForBusinessConnections =     40_000_000L;
	private static final long lingerForTemeletryConnections = 2_000_000_000L;
	
	private static final int NO_LENGTH_DEFINED = -2;
	private static final int SIZE_OF_BEGIN = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_BEGIN_208);
	private static final int SIZE_OF_PLAIN = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210);
	private static final int MAX_URL_LENGTH = 4096;

    private static Logger logger = LoggerFactory.getLogger(HTTP1xRouterStage.class);

    public static boolean showHeader = false; //set to true for debug to see headers in console.
    

    private TrieParserReader trieReader;
   
    private ErrorReporter errorReporter = new ErrorReporter() {

			@Override
			public boolean sendError(long id, int errorCode) {				
				return HTTP1xRouterStage.this.sendError(id, idx-1, errorCode); 	
			}
     	
     };
     
    
    private final Pipe<NetPayloadSchema>[] inputs;
    private final Pipe<HTTPLogRequestSchema> log;
    
    private       long[]                   inputChannels;
    private       int[]                    inputBlobPos;
    private       int[]                    inputBlobPosLimit;
    
    private       int[]                    inputLengths;

    private int[] releaseProducerPipeIndex;
	private long[] releaseTime;
	private long[] releaseChannel;
	private long[] releaseInputSlabPos;
	private int[]  releaseSequences;
	
    //pipes are held here when we have parsed the beginning of this request but its
    //target pipe is full, there is no point in re-parsing the message until this 
    //pipe has room again, also this will be set to null once cleared.
    private Pipe<HTTPRequestSchema>[] blockedOnOutput;
        
    private final Pipe<ReleaseSchema> releasePipe;
    
    private final Pipe<HTTPRequestSchema>[] outputs;
    
    private long[] inputSlabPos;
    private int[] sequences;
        
    public static int MAX_HEADER = 1<<15; //universal maximum header size.
    
    private int[] inputCounts;
    
    private int totalShortestRequest;
    private int shutdownCount;
    
    private int idx;
    private final HTTPRouterStageConfig<T,R,V,H> config;
    private final ServerCoordinator coordinator;
    private final Pipe<ServerResponseSchema> errorResponsePipe;
	private boolean catchAll;
    private final int parallelId;
    private GraphManager gm;//temp needed for startup;
    
    
    private final PipeWorkWatcher pww = new PipeWorkWatcher();
    
    
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
                                           HTTPRouterStageConfig<T,R,V,H> config, 
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
		                           HTTPRouterStageConfig<T,R,V,H> config, 
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
            HTTPRouterStageConfig<T,R,V,H> config, ServerCoordinator coordinator, boolean catchAll) {
		
		this(gm, parallelId, input, join(outputs), errorResponsePipe, log, ackStop, config, coordinator, catchAll);
		
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
			                 Pipe<ServerResponseSchema> errorResponsePipe, 
			                 Pipe<HTTPLogRequestSchema> log,
			                 Pipe<ReleaseSchema> ackStop,
                             HTTPRouterStageConfig<T,R,V,H> config, 
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
        
        //batch the release publishes
        if (inputs.length>=4) {
        	Pipe.setPublishBatchSize(ackStop, 2);
        }
        
        this.outputs = outputs;
        this.coordinator = coordinator;
        this.errorResponsePipe = errorResponsePipe;
        this.catchAll = catchAll;
        assert(outputs.length>=0) : "must have some target for the routed REST calls";
        
        this.shutdownCount = inputs.length;
        this.supportsBatchedPublish = false;
        this.supportsBatchedRelease = false;
        
        GraphManager.addNota(gm, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
           
        this.gm = gm;
        
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

        inputSlabPos = new long[inputs.length];
        sequences = new int[inputs.length];
        
        blockedOnOutput = new Pipe[inputs.length];
        
    	releaseTime = new long[inputs.length];
    	releaseChannel = new long[inputs.length];
    	releaseInputSlabPos = new long[inputs.length];
    	releaseSequences = new int[inputs.length];
    	
    	releaseProducerPipeIndex = new int[inputs.length];
    	int i = inputs.length;
    	while (--i>=0) {
    		
    		int prodId = GraphManager.getRingProducerStageId(gm, inputs[i].id);
    		
    		int outCount = GraphManager.getOutputPipeCount(gm, prodId);
    		for(int j = 1; j <= outCount; j++) {
    			
    			if ( GraphManager.getOutputPipe(gm, prodId, j).id == inputs[i].id) {
    				//we found the pipe so store it
    				//TODO: not sure these numbers are right
    				//      can only test after we start sending them...
    				releaseProducerPipeIndex[i] = j-1; //index position for this pipe...    						
    						
    				break;
    			};
    		}
    	}
    	gm = null;//no longer needed
    	
    	
      
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
        
        pww.init(inputs);    
        
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
    
    	if (pww.hasWork()) {
    		int g = pww.groups();
    		while (--g >= 0) {
    			
    			if (PipeWorkWatcher.scan(pww, g)) {
    		
    				int start = PipeWorkWatcher.getStartIdx(pww, g);
    				int limit = PipeWorkWatcher.getLimitIdx(pww, g);
    				
    				for(int i = start; i<limit; i++) {
    	
    					{
    						
	    					int x=singlePipe(this, i);
	        				if (x >= 0) {      
	        					
	        				} else {
	        					if (-1==x) {
	        						break;//need to wait for full pipe to empty
	        					} else {
	        						assert(x==-2);
	        						if (--shutdownCount<=0) {
	        							requestShutdown();
	        							return;
	        						}
	        					}
	        				}
	        				
	        				releaseIfNeeded(i);	      
	    					   				
	        				
	        				//pending release so we do not consider pipes empty
	        				if (this.releaseInputSlabPos[i]>=0) {
	        					PipeWorkWatcher.setTailPos(pww, i, Pipe.getWorkingTailPosition(inputs[i])); //TODO:pass this in?
	        			
	    					}
    					}
        			
    				}
    				
       			}
    		}
    	
    	}
    	
    }

    public void releaseIfNeeded(int idx) {
  
    	long pos = releaseInputSlabPos[idx];
    	if (pos>=0) {
    		
    		long limit = releaseTime[idx] 
    				    + (isMonitor() ? lingerForTemeletryConnections : lingerForBusinessConnections); //only release after 20ms of non use.
    		if (Pipe.hasRoomForWrite(releasePipe) && System.nanoTime()>limit) {
    							
					int s = Pipe.addMsgIdx(releasePipe, ReleaseSchema.MSG_RELEASEWITHSEQ_101);
					
					//TODO: which is the pipe we took this off off from the sender??
					
					
					Pipe.addLongValue(releaseChannel[idx], releasePipe);
					Pipe.addLongValue(pos, releasePipe);
					Pipe.addIntValue(releaseSequences[idx], releasePipe); //send current sequence number so others can continue at this count.
				
					Pipe.confirmLowLevelWrite(releasePipe, s);
					Pipe.publishWrites(releasePipe);
					
					this.releaseInputSlabPos[idx]=-1;
    		}
    	}
    }


    
    private static int singlePipe(HTTP1xRouterStage<?, ?, ?, ?> that, final int idx) {
 		    	
		        if (accumRunningBytes(that, idx, that.inputs[idx], that.inputChannels[idx]) >=0 ) {//message idx   
		        	//return 0, 1 work, -1 noroom -2 shutdown.
		        	if (that.inputLengths[idx]==0) {
		        		return 0;
		        	}		        	
		        	return parsePipe(that, idx);
		        } else {
		        	//we got -1 msgIdx so shut down
		        	return -2;
		        }     
    }


	private static int parsePipe(HTTP1xRouterStage<?, ?, ?, ?> that, final int idx) {
		//we can accumulate above but we can not parse or continue until this pipe is clear    
	   	if (null == that.blockedOnOutput[idx]) {	    	  
	        return ((that.inputChannels[idx]) < 0) ? 0 : that.parseAvail(idx);
    	} else {
    		if (Pipe.hasRoomForWrite(that.blockedOnOutput[idx])) {
    			that.blockedOnOutput[idx] = null;
    			return 1;
    		} else {
    			return 0;
    		}
    	}
	}



	private int parseAvail(final int idx) {
		
		
		final long channel = inputChannels[idx];
				
        if (null != log && !Pipe.hasRoomForWrite(log)) {
        	return -1;//try later after log pipe is cleared
        }
        
		boolean webSocketUpgraded = ServerCoordinator.isWebSocketUpgraded(coordinator, channel);
		if (!webSocketUpgraded) {			
			return parseHTTPAvail(this, idx, channel);
		} else {
			Pipe<NetPayloadSchema> selectedInput = inputs[idx];
			final int totalAvail = inputLengths[idx];
            final int pipeIdx = ServerCoordinator.getWebSocketPipeIdx(coordinator, channel);
            final Pipe<HTTPRequestSchema> outputPipe = outputs[pipeIdx];
            if (!Pipe.hasRoomForWrite(outputPipe) ) {
             	return -1;//exit to take a break while pipe is full.
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
	            	
	            	return commitWrite(idx, selectedInput, channel, totalAvail,
	            			outputPipe, backing, mask, pos, finOpp,
	            			headerSize, msk, length) ? 1 :0;
	            } else {       
	            	
	            	return 0;//this is the only way to pick up more data, eg. exit the outer loop with zero.
	            }
            } else {
            	
            	
            	return 0;//this is the only way to pick up more data, eg. exit the outer loop with zero.

            }
		}

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
		
		//int totalConsumed = (int)(length+headerSize);
		
		return false;//this is the only way to pick up more data, eg. exit the outer loop with zero.
	}


	private static int parseHTTPAvail(HTTP1xRouterStage that, 
			final int idx,  
			final long channel) {
		
			
		int result = 0;
		//NOTE: we start at the same position until this gets consumed
		TrieParserReader.parseSetup(that.trieReader, 
				                    Pipe.blob(that.inputs[idx]), 
				                    that.inputBlobPos[idx], 
				                    that.inputLengths[idx], 
				                    Pipe.blobMask((that.inputs[idx])));	

//		//drop closed connections
//		ServerConnection cc = that.coordinator.lookupConnectionById(channel);
//		if (cc!=null && !cc.isValid() && cc.getPoolReservation()!=-1) {			
//			TrieParserReader.parseSkip(that.trieReader, that.inputLengths[idx]);			
//			cc.clearPoolReservation();		
//			that.sendRelease(channel, idx);
//			return true;
//		}
				
		int iteration = 0;
		
		do {
			assert(that.inputLengths[idx]>=0) : "length is "+that.inputLengths[idx];

			int totalAvail = that.inputLengths[idx];
			final long toParseLength = TrieParserReader.parseHasContentLength(that.trieReader);
			assert(toParseLength>=0) : "length is "+toParseLength+" and input was "+totalAvail;
			            
			if (toParseLength>0 && (null==that.log || Pipe.hasRoomForWrite(that.log))) {

				long arrivalTime = -1;
		        
		        int seqForLogging = that.sequences[idx];
		        int posForLogging = that.trieReader.sourcePos;
		        
		        int state;
		    	if (that.trieReader.sourceLen > 0) {
		    		arrivalTime = extractArrivalTime((Pipe<NetPayloadSchema>) that.inputs[idx]);
		    		state = that.parseHTTPFromTop(that.trieReader, channel, idx, arrivalTime, iteration++, (Pipe<NetPayloadSchema>) that.inputs[idx]);	
		    		
		    	} else {
		    		state = NEED_MORE_DATA;
		    	}
				
		
				if (SUCCESS == state) {
					
					int totalConsumed = (int)(toParseLength - TrieParserReader.parseHasContentLength(that.trieReader));           
					int remainingBytes = that.trieReader.sourceLen;
					
					if (null != that.log) {						
						logTraffic(that, (Pipe<NetPayloadSchema>) that.inputs[idx], channel, arrivalTime, seqForLogging, posForLogging,
								totalConsumed);
					}
					//inputLengths is updated to become zero...
					result |= consumeBlock(that, idx, (Pipe<NetPayloadSchema>) that.inputs[idx], channel,
							              that.inputBlobPos[idx], 
							              totalAvail, totalConsumed, arrivalTime,
							              remainingBytes) ? 1 : 0;
					
				 } else if (NEED_MORE_DATA == state) {				
					return result;
					   
				 } else {	
					 return -1; 
			     }
			} else {
				assert(that.inputLengths[idx] <= (((Pipe<NetPayloadSchema>) that.inputs[idx]).blobMask+(((Pipe<NetPayloadSchema>) that.inputs[idx]).blobMask>>1))) : "This data should have been parsed but was not understood, check parser and/or sender for corruption.";
				return 0;
			}         
		} while (true); //this while is ok because the TrieReader is returned to its starting position upon failure.
	
	}


	private static boolean validateParseEnd(HTTP1xRouterStage that, final int idx) {
		if (0==TrieParserReader.parseHasContentLength(that.trieReader) || 0==that.inputLengths[idx]) {//(that.inputLengths[idx]-len2)) {
			//the last byte we jsut parsed if everything is empty must be the \n marker
			that.trieReader.moveBack(1);
			return 10==that.trieReader.parseSkipOne();
			
		}
		return true;
	}


	private static void logTraffic(HTTP1xRouterStage that, Pipe<NetPayloadSchema> selectedInput, final long channel,
			long arrivalTime, int seqForLogging, int posForLogging, int totalConsumed) {
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

	private static boolean consumeBlock(HTTP1xRouterStage that, final int idx, 
			Pipe<NetPayloadSchema> selectedInput,
			final long channel, int p, int totalAvail, 
			int totalConsumed, long arrivalTime, int remainingBytes) {
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
		
		if ((totalAvail==0) 
			&& (remainingBytes<=0) 
			&& consumedAllOfActiveFragment(selectedInput, p)
			) {
				that.inputChannels[idx] = -1;//cleared very often

				//proposed for clearing
				assert(0==Pipe.releasePendingByteCount(selectedInput));
				assert(Pipe.getWorkingTailPosition(selectedInput) == Pipe.tailPosition(selectedInput));
				
				if (that.releaseInputSlabPos[idx]>=0 && that.releaseChannel[idx]!=channel) {
					//we have something to release and it does not belong to this channel
					//we must release this now before we add the new value
					
					Pipe.presumeRoomForWrite(that.releasePipe);
					
					int s = Pipe.addMsgIdx(that.releasePipe, ReleaseSchema.MSG_RELEASEWITHSEQ_101);
					
					Pipe.addLongValue(that.releaseChannel[idx], that.releasePipe);
					Pipe.addLongValue(that.releaseInputSlabPos[idx], that.releasePipe);
					Pipe.addIntValue(that.releaseSequences[idx], that.releasePipe); //send current sequence number so others can continue at this count.
				
					Pipe.confirmLowLevelWrite(that.releasePipe, s);
					Pipe.publishWrites(that.releasePipe);					
				}
				
				//do not release now instead store for release later
								
				that.releaseTime[idx] = arrivalTime;
				that.releaseChannel[idx] = channel;
				that.releaseInputSlabPos[idx] = that.inputSlabPos[idx];
				that.releaseSequences[idx] = that.sequences[idx];
								
				that.inputSlabPos[idx] = -1;
				
		}
		return totalConsumed>0;
	}


	private static long extractArrivalTime(Pipe<NetPayloadSchema> selectedInput) {
		long pos = Pipe.tailPosition(selectedInput);
		final long head = Pipe.headPosition(selectedInput);
		
		int id = Pipe.readInt(Pipe.slab(selectedInput), 
			         		  Pipe.slabMask(selectedInput),
			         		  pos);
				
		//skip over messages which do not contain any arrival time, should only be 1 or none.
		while (id!=NetPayloadSchema.MSG_PLAIN_210 
		     && id!=NetPayloadSchema.MSG_ENCRYPTED_200 
		     && pos<head) {
			
			if ((NetPayloadSchema.MSG_BEGIN_208 == id)
				|| (NetPayloadSchema.MSG_DISCONNECT_203 == id)
				|| (NetPayloadSchema.MSG_UPGRADE_307 == id)					
					) {
				pos += Pipe.sizeOf(NetPayloadSchema.instance, id);			
				id = Pipe.readInt(Pipe.slab(selectedInput), Pipe.slabMask(selectedInput), pos);
			} else {
				//unknown data in the pipe, this should not happen
				logger.trace("unable to capture arrival time for {} at pipe {} ",id, selectedInput);
				return -1;
			}
		}
		
		if (pos<head && (id==NetPayloadSchema.MSG_PLAIN_210 || id==NetPayloadSchema.MSG_ENCRYPTED_200) ) {
			return Pipe.readLong(Pipe.slab(selectedInput), 
											 Pipe.slabMask(selectedInput),
											 pos + 3);

		} else {
			logger.info("unable to capture arrival time for {}",id);
			return -1;
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

		return (Pipe.blobMask(selectedInput)&Pipe.getWorkingBlobTailPosition(selectedInput)	) == (Pipe.blobMask(selectedInput)&p) && 
		       (Pipe.blobMask(selectedInput)&Pipe.getWorkingBlobHeadPosition(selectedInput)	) == (Pipe.blobMask(selectedInput)&p);
		
	}

    
 private final static int NEED_MORE_DATA = 2;
 private final static int SUCCESS = 1;

 
 
// -1 no room on output pipe (do not call again until the pipe is clear, send back pipe to watch)
//  2 need more data to parse (do not call again until data to parse arrives)
//  1 success
// <=0 for wating on this output pipe to have room (the pipe idx is negative)
 
 
private int parseHTTPFromTop(TrieParserReader trieReader, final long channel, final int idx, long arrivalTime, 
		int iteration, Pipe<NetPayloadSchema> selectedInput) {    


	//keep this since we will start at the top again
	final int tempLen = trieReader.sourceLen;
	final int tempPos = trieReader.sourcePos;

	if (showHeader) {
		if (!"Telemetry Server".equals(coordinator.serviceName())) {//do not show for monitor
			int lenLimit = Math.min(8192, trieReader.sourceLen);
	    	System.out.println("///////////////// DATA TO ROUTE (HEADER+) "+channel+" pos:"+trieReader.sourcePos+" len:"+lenLimit+"///////////////////");
			TrieParserReader.debugAsUTF8(trieReader, System.out, lenLimit, false); //shows that we did not get all the data
	    	if (trieReader.sourceLen>8192) {
	    		System.out.println("...");
	    	}
	    	System.out.println("\n///////////////////////////////////////////");
		}
    }

    
	final int verbId = (int)TrieParserReader.parseNext(trieReader, config.verbMap, -1, -2);     //  GET /hello/x?x=3 HTTP/1.1     
    if (verbId<0) {
    		if (-1==verbId && (trieReader.sourceLen < (config.verbMap.longestKnown()+1) )) { //added 1 for the space which must appear after
    			
    			//we are staying here reading more data so we must restore this before we continue
    			trieReader.sourceLen = tempLen;
    			trieReader.sourcePos = tempPos;
    			
    			return NEED_MORE_DATA;    			
    		} else {
    			//the -2 unFound case and the case where we have too much data.
    			badVerbParse(trieReader, channel, idx, tempLen, tempPos);
    			return SUCCESS;			
    		}    		
    }
    
   // System.err.println("start at pos "+tempPos+" for "+channel);
    
    final boolean showTheRouteMap = false;
    if (showTheRouteMap) {
    	config.debugURLMap();
    }
    
	final int pathId = (int)TrieParserReader.parseNext(trieReader, config.urlMap, -1, -2);     //  GET /hello/x?x=3 HTTP/1.1 
	
	if (pathId<0) {
		
		if (-1==pathId && trieReader.sourceLen < config.urlMap.longestKnown() ) {
			//we are staying here reading more data so we must restore this before we continue
			trieReader.sourceLen = tempLen;
			trieReader.sourcePos = tempPos;
			//logger.info(routeId+" need more data C  "+tempLen+"  "+config.urlMap.longestKnown()+" "+trieReader.sourceLen);
			return NEED_MORE_DATA;    			
		} else {
			//bad format route path error, could not find space after path and before route, send 404 error
			sendError(trieReader, channel, idx, tempLen, tempPos, 404);	
			return SUCCESS;
		}
	}	
	
	//the above URLS always end with a white space to ensure they match the spec.
    int routeId;
    if (config.UNMAPPED_ROUTE == pathId) {
	    if (!catchAll) {
	    	
			//unsupported route path, send 404 error			
	    	sendError(trieReader, channel, idx, tempLen, tempPos, 404);	
	    	
			return SUCCESS;
	    }
    	routeId = config.UNMAPPED_ROUTE; 
    } else {
    	routeId = config.getRouteIdForPathId(pathId);
    }
    
    //NOTE: many different routeIds may return the same outputPipe, since they all go to the same palace
    //      if catch all is enabled use it because all outputs will be null in that mode
    Pipe<HTTPRequestSchema> outputPipe = routeId<outputs.length ? outputs[routeId] : outputs[0];
    if (Pipe.hasRoomForWrite(outputPipe) ) { //NOTE: can we do this much earlier, TODO: may save some compute time...
    	//if thie above code went past the end OR if there is not enough room for an empty header  line maker then return
    	if (trieReader.sourceLen<2) { 
    		//we are staying here reading more data so we must restore this before we continue
			trieReader.sourceLen = tempLen;
			trieReader.sourcePos = tempPos;
			
    		return NEED_MORE_DATA;
    	}    	
    	
    	int result = parseHTTPImpl(trieReader, channel, idx, arrivalTime, tempLen, tempPos, verbId, pathId, routeId, outputPipe);
    	if (result != SUCCESS) {
    		trieReader.sourceLen = tempLen;
			trieReader.sourcePos = tempPos;
    	}
    	return result;
    } else {
    	blockedOnOutput[idx] = outputPipe;//block on this pipe until it gets room again, do not parse until then.
    	//we are staying here reading more data so we must restore this before we continue
		trieReader.sourceLen = tempLen;
		trieReader.sourcePos = tempPos;
    	return -1;//no room so do not parse and try again later.
    }
   // logger.info("send this message to route {}",routeId);
    
}


private void badVerbParse(TrieParserReader trieReader, final long channel, final int idx, int tempLen, int tempPos) {

	//trieReader.debugAsUTF8(trieReader, System.out);
	logger.trace("bad HTTP data recieved by server, channel will be closed. Bytes abandoned:{} looking for verb at {} ",tempLen, tempPos);
	sendError(trieReader, channel, idx, tempLen, tempPos, 400);	
	
	//we have bad data we have been sent, there is enough data yet the verb was not found
	
	final boolean debug = false;
	if(debug) {
		trieReader.sourceLen = tempLen;
		trieReader.sourcePos = tempPos; 
		logger.warn("error at position: {} length: {}",trieReader.sourcePos, trieReader.sourceLen);
		StringBuilder builder = new StringBuilder();    			    			
		TrieParserReader.debugAsUTF8(trieReader, builder, config.verbMap.longestKnown()*2);    			
		logger.warn("{} looking for verb but found:\n{} at position {} \n\n",channel,builder,tempPos);
	}
	
	trieReader.sourceLen = 0;
	trieReader.sourcePos = 0;    			
	
	BaseConnection con = coordinator.lookupConnectionById(channel);
	if (null!=con) {
		con.clearPoolReservation();
	}
	//logger.info("success");
}


private int parseHTTPImpl(TrieParserReader trieReader, final long channel, final int idx, long arrivalTime,
		int tempLen, int tempPos, final int verbId, final int pathId, int routeId, Pipe<HTTPRequestSchema> outputPipe) {
	
		Pipe.markHead(outputPipe);//holds in case we need to abandon our writes
		//NOTE: caller checks for room before calling.
		
		Pipe.presumeRoomForWrite(outputPipe);
        final int size =  Pipe.addMsgIdx(outputPipe, HTTPRequestSchema.MSG_RESTREQUEST_300);        // Write 1   1                         
        Pipe.addLongValue(channel, outputPipe); // Channel                        // Write 2   3        
        Pipe.addIntValue(sequences[idx], outputPipe); //sequence                    // Write 1   4
        
        //route and verb
        Pipe.addIntValue((routeId << HTTPVerb.BITS) | (verbId & HTTPVerb.MASK), outputPipe);// Verb                           // Write 1   5
        
        DataOutputBlobWriter<HTTPRequestSchema> writer = Pipe.openOutputStream(outputPipe);      //write 2   7
      
        int structId;
        TrieParser headerMap;
        
        if (config.UNMAPPED_ROUTE != pathId) {
        	
        	structId = config.extractionParser(pathId).structId;
        	assert(config.getStructIdForRouteId(routeId) == structId) : "internal error";
            DataOutputBlobWriter.tryClearIntBackData(writer,config.totalSizeOfIndexes(structId));
          	if (!captureAllArgsFromURL(config, trieReader, pathId, writer)) {          		
          		return sendError(trieReader, channel, idx, tempLen, tempPos);
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
        int httpRevisionId = (int)TrieParserReader.parseNext(trieReader, config.revisionMap, -1, -2);  //  GET /hello/x?x=3 HTTP/1.1 
        if (httpRevisionId<0) { 
        	return noRevisionProcessing(trieReader, channel, idx, tempLen, tempPos, outputPipe, writer, httpRevisionId);
        }
        
		////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////also write the requested headers out to the payload
        /////////////////////////////////////////////////////////////////////////
  
        //logger.info("extractions before headers count is {} ",config.extractionParser(routeId).getIndexCount());
        

        //	int countOfAllPreviousFields = extractionParser.getIndexCount()+indexOffsetCount;
		ServerConnection serverCon = coordinator.<ServerConnection>lookupConnectionById(channel);
		
		int requestContext = ServerCoordinator.INCOMPLETE_RESPONSE_MASK;

		int headPos = -1;
		if (serverCon!=null) {

			assert (routeId == config.getRouteIdForPathId(pathId));
			
			if (serverCon.hasDataRoom()) {
			
				headPos = serverCon.enqueueStartTime(arrivalTime);
				
				requestContext = parseHeaderFields(trieReader, pathId, headerMap, writer, serverCon, 
													httpRevisionId, config,
													errorReporter, arrivalTime, channel);  // Write 2   10 //if header is p
			
				
			} else {
				logger.warn("too many requests already in flight in server, limit may be 32K per connection...");
				DataOutputBlobWriter.closeLowLevelField(writer);
	            //try again later, not complete.
	            Pipe.resetHead(outputPipe);
	            return -1;//the downstream stages need to consume this now.
			}
		
		} 
        
        if (ServerCoordinator.INCOMPLETE_RESPONSE_MASK == requestContext) {         	

        	DataOutputBlobWriter.closeLowLevelField(writer);
            //try again later, not complete.
            Pipe.resetHead(outputPipe);
            return NEED_MORE_DATA;
        } else {
        	if (-1 != headPos) {
        		serverCon.publishStartTime(headPos);
        	}
        	//nothing need be done to commit the data writes.   	
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
    
	    inputCounts[idx]++; 
	    return SUCCESS;
}


private int noRevisionProcessing(TrieParserReader trieReader, final long channel, final int idx, int tempLen,
		int tempPos, Pipe<HTTPRequestSchema> outputPipe, DataOutputBlobWriter<HTTPRequestSchema> writer,
		int httpRevisionId) {
	DataOutputBlobWriter.closeLowLevelField(writer);
	Pipe.resetHead(outputPipe);
	int result = -1;
	if (-1==httpRevisionId && (tempLen < (config.revisionMap.longestKnown()+1) || (trieReader.sourceLen<0) )) { //added 1 for the space which must appear after
		//logger.info("need more data D");        		
		result = NEED_MORE_DATA;    			
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
		
		BaseConnection con = coordinator.lookupConnectionById(channel);
		if (null!=con) {
			con.clearPoolReservation();
		}
		//logger.info("success");
		result =  SUCCESS;
	}
	return result;
}


private int sendError(TrieParserReader trieReader, final long channel, final int idx, int tempLen, int tempPos) {
	//////////////////////////////////////////////////////////////////
	//did not pass validation so we return 404 without giving any clues why
	//////////////////////////////////////////////////////////////////
	sendError(trieReader, channel, idx, tempLen, tempPos, 404);	
	trieReader.sourceLen = 0;
	trieReader.sourcePos = 0;
	
	BaseConnection con = coordinator.lookupConnectionById(channel);
	if (null!=con) {
		con.clearPoolReservation();
	}
	
	return SUCCESS;
}

private static boolean captureAllArgsFromURL(HTTPRouterStageConfig<?,?,?,?> config,
		                                     TrieParserReader trieReader, final int pathId,
										     DataOutputBlobWriter<HTTPRequestSchema> writer) {

	final FieldExtractionDefinitions fed = HTTPRouterStageConfig.fieldExDef(config,pathId);
	
	if (0 == fed.paramIndexArray().length) {
		//load any defaults
		fed.processDefaults(writer);
		return true;
	} else {	
		if (TrieParserReader.writeCapturedValuesToDataOutput(trieReader, writer, 
				                                         fed.paramIndexArray(),
				                                         fed.paramIndexArrayValidator()
														)) {
			//load any defaults if what was passed in has been validated
			fed.processDefaults(writer);
			return true;
		}
	}
	return false;
}


private static int parseHeaderFields(TrieParserReader trieReader, 
		final int pathId, final TrieParser headerMap, 
		DataOutputBlobWriter<HTTPRequestSchema> writer, 
		ServerConnection serverConnection,
		int httpRevisionId,
		HTTPRouterStageConfig<?, ?, ?, ?> config,
		ErrorReporter errorReporter2, long arrivalTime, long ccId) {
	
			int requestContext = keepAliveOrNotContext(httpRevisionId, serverConnection.id);
	
			long postLength = NO_LENGTH_DEFINED;
			
			int iteration = 0;
			int remainingLen;
			while ((remainingLen=TrieParserReader.parseHasContentLength(trieReader))>0){
			
				//this may be an unknown header so we allow for non matching from our list.
				long headerToken = TrieParserReader.parseNext(trieReader, headerMap);
				
			    if (HTTPSpecification.END_OF_HEADER_ID == headerToken) { 
			    	return endOfHeader(trieReader, writer,
			    			errorReporter2, arrivalTime, ccId, requestContext,
							postLength, iteration);
			    	
			    } else if (-1 == headerToken) {            	
			    	return noHeaderToken(serverConnection, errorReporter2, ccId, requestContext, remainingLen); 
			    } else if (HTTPSpecification.UNKNOWN_HEADER_ID != headerToken) {	    		
				    HTTPHeader header = config.getAssociatedObject(headerToken);
				    int writePosition = writer.position();
				    
				    if (null!=header) {
				    	int echoIndex = -1;
				    	if ((echoIndex = serverConnection.scs.isEchoHeader(header)) >= 0 ) {
				    		//write this to be echoed when responding.
				    		
				//				    		DataOutputBlobWriter.setIntBackData((DataOutputBlobWriter<?>)cw, 
				//				    				cw.position(), StructRegistry.FIELD_MASK & (int)headerToken);											    		 
				    		//TrieParserReader.writeCapturedValuesToDataOutput(trieReader, null);
				    		
				    		//serverConnection.storeHeader(echoIndex, ); ///TODO: how
				    		
				    		throw new UnsupportedOperationException("Echo headers not yet implmented...");
	
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
		
	
	return ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
}


private static int endOfHeader(TrieParserReader trieReader, DataOutputBlobWriter<HTTPRequestSchema> writer,
		ErrorReporter errorReporter2, long arrivalTime, long ccId, int requestContext, long postLength, int iteration) {

		if (iteration!=0) {						    	
			
			//we explicitly set length to zero if it is not provided.
			return endOfHeadersLogic(writer, 
					errorReporter2, requestContext, 
					trieReader, NO_LENGTH_DEFINED == postLength ? 0 : postLength,
					arrivalTime, ccId);
			
		} else {
			//needs more data 						
			return ServerCoordinator.INCOMPLETE_RESPONSE_MASK;                	
		}
	
}


private static int noHeaderToken(ServerConnection serverConnection, ErrorReporter errorReporter2, long ccId,
		int requestContext, int remainingLen) {
	if (remainingLen>MAX_HEADER) {   
		//client has sent very bad data.
		logger.info("client has sent bad data connection {} was closed",serverConnection.id);
		return errorReporter2.sendError(ccId, 400) ? (requestContext | ServerCoordinator.CLOSE_CONNECTION_MASK) : ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
	}
	//nothing valid was found so this is incomplete.
	
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

	BaseConnection con = coordinator.lookupConnectionById(channel);
	boolean sent = sendError(channel, idx, errorCode);
	if (sent) {
		//error was sent, do clear reservation 
//		if (null!=con) {
//			con.clearPoolReservation();	
//		}
	} else {
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
		
		sequences[idx]++;
		sent = true;				
	}

	sendRelease(channel, idx);

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
	if (inputSlabPos[idx]>=0) {
		Pipe.presumeRoomForWrite(releasePipe);
		
		int s = Pipe.addMsgIdx(releasePipe, ReleaseSchema.MSG_RELEASEWITHSEQ_101);
		
		Pipe.addLongValue(channel, releasePipe);
		Pipe.addLongValue(inputSlabPos[idx], releasePipe);
		Pipe.addIntValue(sequences[idx], releasePipe); //send current sequence number so others can continue at this count.
	
		Pipe.confirmLowLevelWrite(releasePipe, s);
		Pipe.publishWrites(releasePipe);
		
		this.inputSlabPos[idx]=-1;
	}
}

private static int accumRunningBytes(
						 HTTP1xRouterStage<?, ?, ?, ?> that,
						 final int idx, 
		                 Pipe<NetPayloadSchema> selectedInput,
		                 long inChnl) {

	    int messageIdx = Integer.MAX_VALUE;

	    //data may be sent in small chunks even when we only have 1 message in flight
	    int maxIter = 2;
	    while (--maxIter>=0 && hasDataToAccum(selectedInput, inChnl)) {
	    	//if the old was released or this matches or this was proposed for release we set this new current channel.
	    	
	    	messageIdx = Pipe.takeMsgIdx(selectedInput);

	        //logger.info("seen message id of {}",messageIdx);
	        
	        if (NetPayloadSchema.MSG_PLAIN_210 == messageIdx) {
	        	inChnl = processPlain(that, idx, selectedInput, inChnl);
	        } else {	        	
	        	if (NetPayloadSchema.MSG_BEGIN_208 == messageIdx) {
	        		processBegin(that, idx, selectedInput);
	        	} else {
	        		if (NetPayloadSchema.MSG_DISCONNECT_203 == messageIdx) {	 
	        			processDisconnect(that, idx, selectedInput);	        			
	        		} else {	        		
	        			return processShutdown(selectedInput, messageIdx);
	        		}
	        	}
	        }        
	    }
	  
	    return messageIdx;
}


private static long processPlain(HTTP1xRouterStage<?, ?, ?, ?> that, final int idx,
		Pipe<NetPayloadSchema> selectedInput, long inChnl) {

	long channel = Pipe.takeLong(selectedInput);
	if (inChnl<0) {
		that.inputChannels[idx] = channel;
	}
	
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
	
	if (-1 != inChnl && that.inputLengths[idx]!=0) {
		//System.out.println("added length "+length);
		that.plainMatch(idx, selectedInput, channel, length);
	} else {
		that.plainFreshStart(idx, selectedInput, channel, length, pos);	
	}
		
	assert(that.inputLengths[idx]>=0) : "error negative length not supported";
	
	//if we do not move this forward we will keep reading the same spot up to the new head
	Pipe.confirmLowLevelRead(selectedInput, SIZE_OF_PLAIN);            
	Pipe.readNextWithoutReleasingReadLock(selectedInput);
	
	if (-1 == slabPos) {
		that.inputSlabPos[idx] = Pipe.getWorkingTailPosition(selectedInput); 
		//working and was tested since this is low level with unrleased block.
	}
	assert(that.inputSlabPos[idx]!=-1);
	
	return channel;
}


private static boolean hasDataToAccum(Pipe<NetPayloadSchema> selectedInput, long inChnl) {
	return Pipe.hasContentToRead(selectedInput)
			&& (    //content checked first to ensure asserts pass		
				hasContinuedData(selectedInput, inChnl) ||
				hasNoActiveChannel(inChnl) ||      //if we do not have an active channel
				hasReachedEndOfStream(selectedInput) 
				//if we have reached the end of the stream       
	    );
}


private static void processDisconnect(HTTP1xRouterStage<?, ?, ?, ?> that, final int idx,
		Pipe<NetPayloadSchema> selectedInput) {
	long connectionId = Pipe.takeLong(selectedInput);

	BaseConnection con = that.coordinator.lookupConnectionById(connectionId);
	if (null!=con) {
		if (con instanceof ClientConnection) {
			((ClientConnection)con).beginDisconnect();
		}
		
		con.clearPoolReservation();		
		con.close();
		that.sendRelease(connectionId, idx);
			        				
	}	        			
	Pipe.confirmLowLevelRead(selectedInput, Pipe.sizeOf(selectedInput, NetPayloadSchema.MSG_DISCONNECT_203));
	//Pipe.releaseReadLock(selectedInput);
	Pipe.readNextWithoutReleasingReadLock(selectedInput);
	Pipe.releasePendingAsReadLock(selectedInput, 0);
}

private void plainMatch(final int idx, Pipe<NetPayloadSchema> selectedInput,
		                long channel, int length) {
	//confirm match
	assert(inputChannels[idx] == channel) : "Internal error, mixed channels";
	
	//grow position
	assert(inputLengths[idx]>=0) : "not expected to be 0 or negative but found "+inputLengths[idx];
	inputLengths[idx] += length; 
	inputBlobPosLimit[idx] += length;
	
	assert(inputLengths[idx] < Pipe.blobMask(selectedInput)) : "When we roll up is must always be smaller than ring "+inputLengths[idx]+" is too large for "+Pipe.blobMask(selectedInput);
	
	//may only read up to safe point where head is       
	assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));
}


private void plainFreshStart(final int idx, Pipe<NetPayloadSchema> selectedInput, long channel, int length, int pos) {

	//is freshStart
	assert(inputLengths[idx]<=0) : "expected to be 0 or negative but found "+inputLengths[idx];
	
	//assign
	inputChannels[idx]     = channel;
	inputLengths[idx]      = length;
	inputBlobPos[idx]      = pos;
	inputBlobPosLimit[idx] = pos + length;
	
	//logger.info("added new fresh start data of {}",length);
	assert(inputs[idx] == selectedInput);
	assert(inputLengths[idx]<selectedInput.sizeOfBlobRing);
	assert(Pipe.validatePipeBlobHasDataToRead(selectedInput, inputBlobPos[idx], inputLengths[idx]));

}


	private static int processShutdown(Pipe<NetPayloadSchema> selectedInput, final int messageIdx) {
		assert(-1 == messageIdx) : "messageIdx:"+messageIdx;
		if (-1 != messageIdx) {
			throw new UnsupportedOperationException("bad id "+messageIdx+" raw data  \n"+selectedInput);
		}
		
		Pipe.confirmLowLevelRead(selectedInput, Pipe.EOF_SIZE);
		Pipe.readNextWithoutReleasingReadLock(selectedInput);
		Pipe.releaseAllPendingReadLock(selectedInput);;
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
		
		//starting a new sequence of data, all the old data including this begin must be released.
		Pipe.releasePendingAsReadLock(selectedInput, 0);
	}


    // Warning Pipe.hasContentToRead(selectedInput) must be called first.
	private static boolean hasNoActiveChannel(long inChnl) {
		return inChnl<0;
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
    		ErrorReporter errorReporter,
			int requestContext, final TrieParserReader trieReader,
			long postLength, long arrivalTime, long ccId) {
		//logger.trace("end of request found");
		//THIS IS THE ONLY POINT WHERE WE EXIT THIS MTHOD WITH A COMPLETE PARSE OF THE HEADER, 
		//ALL OTHERS MUST RETURN INCOMPLETE
		
    	if (postLength>=0) {
			//full length is done here as a single call, the pipe must be large enough to hold the entire payload
			
			assert(postLength<=Integer.MAX_VALUE) : "can not support posts larger than 2G at this time";
			
			//read data directly
			final int writePosition = writer.position();  
			
			if (DataOutputBlobWriter.lastBackPositionOfIndex(writer)>=(writePosition+postLength)) {
				int cpyLen = TrieParserReader.parseCopy(trieReader, postLength, writer);
				if (cpyLen<postLength) {
				   //needs more data 
					return ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
				} else {	
					
					//NOTE: payload index is always found at position 0 since it is the first field.
					DataOutputBlobWriter.setIntBackData(writer, writePosition, 0);
	
				}
			} else {
				logger.warn("pipes are too small for this many headers and payload, max size is "+writer.getPipe().maxVarLen);	
				requestContext = errorReporter.sendError(ccId,503) ? (requestContext | ServerCoordinator.CLOSE_CONNECTION_MASK) : ServerCoordinator.INCOMPLETE_RESPONSE_MASK;				
			}

		} else if (DataOutputBlobWriter.lastBackPositionOfIndex(writer)<writer.position()) {				
			
    		logger.warn("pipes are too small for this many headers, max total header size is "+writer.getPipe().maxVarLen);	
			requestContext = errorReporter.sendError(ccId,503) ? (requestContext | ServerCoordinator.CLOSE_CONNECTION_MASK) : ServerCoordinator.INCOMPLETE_RESPONSE_MASK;
		
		}
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
		        //new Exception("close detected").printStackTrace();
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


	public HTTPRouterStageConfig<?,?,?,?> routerConfig() {
		return config;
	}
    
}
