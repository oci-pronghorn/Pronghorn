package com.ociweb.pronghorn.network.http;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.BloomFilter;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

/**
 * Parses HTTP1.x responses from the server and sends an acknowledgment to an output pipe
 * to be sent back to a request stage.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class HTTP1xResponseParserStage extends PronghornStage {
	private static final int SIZE_TO_WRITE = 2*Pipe.sizeOf(NetResponseSchema.instance, NetResponseSchema.MSG_CONTINUATION_102);
	private static final int SIZE_OF_MSG_RESPONSE = Pipe.sizeOf(NetResponseSchema.instance, NetResponseSchema.MSG_RESPONSE_101);
	private final Pipe<NetPayloadSchema>[] input; 
	private final Pipe<NetResponseSchema>[] output;
	
	private long[] inputPosition;
	private long[] arrivalTimeAtPosition;
	private long[] blockedPosition;
	private int[]  blockedOpenCount;
	private int[]  blockedLen;
	private int[]  blockedState;
	
	private final Pipe<ReleaseSchema> releasePipe;
	private final HTTPSpecification<?,?,?,?> httpSpec;
	
	private ClientCoordinator ccm;
	
		
	
	private static final Logger logger = LoggerFactory.getLogger(HTTP1xResponseParserStage.class);
	
	private static final int MAX_VALID_HEADER = 32768; //much larger than most servers using 4-8K (pipe blob must be larger than this)

   	
	private TrieParser revisionMap;
	
	private TrieParserReader trieReader;
	private TrieParserReader trieReaderCharType;
	
	
	private TrieParser chunkEnd;
	
	private int[] positionMemoData;
	private long[] payloadLengthData;
	private boolean[] closeRequested;
	private long[] ccIdData;
	
	private int[] runningHeaderBytes;
	
	public static boolean showData = false;

    /**
     *
     * @param graphManager
     * @param input _in_ Pipe containing the HTTP payload.
     * @param output _out_  Net response.
     * @param ackStop _out_ Acknowledgment for forwarding.
     * @param ccm
     * @param httpSpec
     */
	public HTTP1xResponseParserStage(GraphManager graphManager, 
			                       Pipe<NetPayloadSchema>[] input, 
			                       Pipe<NetResponseSchema>[] output, 
			                       Pipe<ReleaseSchema> ackStop,
			                       ClientCoordinator ccm,
			                       HTTPSpecification<?,?,?,?> httpSpec) {
		
		super(graphManager, input, join(output,ackStop));
		this.input = input;
		this.output =  output.length==1 & input.length>1 ? 
				       reSizeArray(output[0],input.length) //we only have 1 output so all data must go there.
				       : output;//must be 1 for each listener
		
		assert(this.input.length == this.output.length) : "destination pipe is selected when data comes from socket, input and output indexes must match. input:"+input.length+" != output:"+output.length;
		this.ccm = ccm;
		this.releasePipe = ackStop;
		this.httpSpec = httpSpec;		
		  		
		int i = input.length;
		while (--i>=0) {
			assert(	input[i].sizeOfBlobRing >=  MAX_VALID_HEADER*2 ); //size of blob ring is the largest a header can ever be.			
		}

		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lavenderblush", this);
		GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
		GraphManager.addNota(graphManager, GraphManager.LOAD_BALANCER, GraphManager.LOAD_BALANCER, this);
		
		//GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
			
	}

	  private Pipe<NetResponseSchema>[] reSizeArray(Pipe<NetResponseSchema> output, int length) {
		  
		  Pipe<NetResponseSchema>[] result = new Pipe[length];
		  Arrays.fill(result, output);		  
		  return result;
	}

	@Override
	    public void startup() {

		  positionMemoData = new int[input.length<<2];
		  payloadLengthData = new long[input.length];
		  closeRequested = new boolean[input.length];
		  ccIdData = new long[input.length];
		  inputPosition = new long[input.length];
		  arrivalTimeAtPosition = new long[input.length];
		  blockedPosition = new long[input.length];
		  blockedOpenCount = new int[input.length];
		  blockedLen = new int[input.length];
		  blockedState = new int[input.length];
		  
		  runningHeaderBytes = new int[input.length];
		  			  
		  trieReader = new TrieParserReader();
		  trieReaderCharType = new TrieParserReader();//this used inside the other.
		  
		  int x;
		  
		  //HTTP/1.1 200 OK
		  //revision  status# statusString\r\n
		  //headers
		  
	      ///////////////////////////
	      //Load the supported HTTP revisions
	      ///////////////////////////
	      revisionMap = new TrieParser(256,true); //TODO: set switch to turn on off the deep check skip
	      HTTPRevision[] revs = httpSpec.supportedHTTPRevisions.getEnumConstants();
	      x = revs.length;               
	      while (--x >= 0) {
	    	    revisionMap.setUTF8Value(revs[x].getKey(), " 200 OK\r\n", revs[x].ordinal());
	            revisionMap.setUTF8Value(revs[x].getKey(), " %u %b\r\n", revs[x].ordinal());
	           // revisionMap.setUTF8Value(revs[x].getKey(), " %u %b\n", revs[x].ordinal());    //\n must be last because we prefer to have it pick \r\n
	      }
	      
	      chunkEnd = new TrieParser(256,true);
	      chunkEnd.setUTF8Value("\r\n",1);
	            
	      
	    }

	int lastUser;
	int lastState;
	int lastCount;
	int responseCount = 0;
	
	void storeState(int state, int user) {
		if (state==lastState && lastUser == user) {
			lastCount++;
		} else {
			lastCount = 0;
		}
		lastState = state;
		lastUser = user;
	}
		
	@Override
	public void run() {
			process();		
	}

	private void process() {
		int foundWork; //keep going until we make a pass and there is no work.
			
		int maxIter = 10000; //this stage is slow and must be forced to return periodically

		do {		
			foundWork = 0;
			
			
			int i = input.length;
			while (--i>=0) {
			

				final int memoIdx = i<<2; //base index is i  * 4;
				final int posIdx   = memoIdx;
				final int lenIdx   = memoIdx+1;
				final int stateIdx = memoIdx+2;
				
				Pipe<NetResponseSchema> targetPipe = null;
				long ccId = 0;
				
				Pipe<NetPayloadSchema> localInputPipe = input[i];
				
				/////////////////////////////////////////////////////////////
				//ensure we have the right backing array, and mask (no position change)
				/////////////////////////////////////////////////////////////
				TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);

				TrieParserReader.parseSetup(
						     trieReader,
						     Pipe.blob(localInputPipe),
						     trieReader.sourcePos,
						     trieReader.sourceLen,
						     Pipe.blobMask(localInputPipe));							
	
				
				HTTPClientConnection cc = null;
					
				int len1 = positionMemoData[lenIdx];
				int len2 = localInputPipe.maxVarLen;
				int len3 = Pipe.blobMask(localInputPipe);
				
				if (Pipe.hasContentToRead(localInputPipe)) {

					////////////////
					//before taking the data
					//ensure that it can be consumed 
					///////////////
					ccId = Pipe.peekLong(localInputPipe, 1); //works for plain and disconnect.
					boolean alsoReturnDisconnected = true;
					cc = (HTTPClientConnection)ccm.connectionObjForConnectionId(ccId, alsoReturnDisconnected);
					targetPipe = output[i];
					
					//////////////////////////////
					//we have new data to consume
					//////////////////////////////		
					final int msgIdx = Pipe.takeMsgIdx(localInputPipe);
					final int sizeOf = Pipe.sizeOf(localInputPipe, msgIdx);
					if (msgIdx>=0) {//does not read the body if this is a shutdown request.
											
						//is closed
						if (((null==cc) || (!cc.isValid()) || cc.isClientClosedNotificationSent())) {	
									
							//logger.info("\nclosed {} connection detected ",cc);		
							
							Pipe.skipNextFragment(localInputPipe, msgIdx);
							closeConnectionAndAbandonOldData(posIdx, lenIdx, stateIdx, cc, i);							
							continue;
						}
										
						
					    if (msgIdx != NetPayloadSchema.MSG_PLAIN_210) {							
					    	if (msgIdx == NetPayloadSchema.MSG_DISCONNECT_203) {
					    		long connectionId = Pipe.takeLong(localInputPipe);
					    		assert(ccId == connectionId);								
					    		
								Pipe.confirmLowLevelRead(localInputPipe, sizeOf); 
								Pipe.readNextWithoutReleasingReadLock(localInputPipe);	
					    		
					    		if (cc.id == ccId) {
					    			closeConnectionAndAbandonOldData(posIdx, lenIdx, stateIdx, cc, i);	
					    		}
					    		Pipe.releaseAllPendingReadLock(localInputPipe);	
					    		
					    		continue;
					    		
					    	} else {
					    		throw new UnsupportedOperationException("unknown msgIdx: "+msgIdx);
					    	}
						
					    } else {
					    	boolean ok = ccId == Pipe.takeLong(localInputPipe);
					    	assert(ok) : "Internal error";
							
					    	targetPipe = (int)cc.getResponsePipeIdx()>=0? output[i] : null;					
							
					    	
							long arrivalTime = Pipe.takeLong(localInputPipe);
							//if already set do not set again, we want the leading edge of the data arrival.
							if (arrivalTimeAtPosition[i]<=0) {
								arrivalTimeAtPosition[i] = arrivalTime;
							}
							
							inputPosition[i] = Pipe.takeLong(localInputPipe);										
							ccIdData[i] = ccId;
				
							//append the new data
							int meta = Pipe.takeByteArrayMetaData(localInputPipe);
							int len = Math.max(0, Pipe.takeByteArrayLength(localInputPipe));
							int pos = Pipe.bytePosition(meta, localInputPipe, len);
							int mask = Pipe.blobMask(localInputPipe);
							
							///////////////////////////////////
							//logger.info("parse new data of {} for connection {}",len,cc.getId());
							//////////////////////////////////
							
							if (showData) {
								Appendables.appendUTF8(System.out /*capturedContent*/, Pipe.blob(localInputPipe), pos, len, mask);
							}
							///////////////////////////////////////////
							/////////////////////////////////////////
							
							if (positionMemoData[lenIdx]==0) {
								positionMemoData[posIdx] = pos;						
								positionMemoData[lenIdx] = len;								
								validateFirstByteOfHeader(i, posIdx, lenIdx, memoIdx, stateIdx, localInputPipe, cc, len);								
							} else {				
								positionMemoData[lenIdx] += len;
							}

							if (positionMemoData[stateIdx] == 0) {
								assert (null==targetPipe || !Pipe.isInBlobFieldWrite(targetPipe)) : 
									   "for starting state expected pipe to NOT be in blob write";
							}
							
							assert(positionMemoData[lenIdx]<Pipe.blobMask(localInputPipe)) : "error adding "+len+" total was "+positionMemoData[lenIdx]+" and should be < "+localInputPipe.blobMask(localInputPipe);
		
							TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
						}
					}
					
					//done consuming this message.
					Pipe.confirmLowLevelRead(localInputPipe, sizeOf);   //release of read does not happen until the bytes are consumed...
					
					//WARNING: moving next without releasing lock prevents new data from arriving until after we have consumed everything.
					Pipe.readNextWithoutReleasingReadLock(localInputPipe);	
							
					if (    msgIdx == NetPayloadSchema.MSG_DISCONNECT_203 
					    ||	msgIdx == NetPayloadSchema.MSG_BEGIN_208) {
						//these operations have no payload bytes to cause a release plus they mark an endpoint where we can reset safely
						Pipe.releaseAllPendingReadLock(localInputPipe);						
					}
					
					if (-1==inputPosition[i]) {
						//this may or may not be the end of a complete message, must hold it just in case it is.
						inputPosition[i] = Pipe.getWorkingTailPosition(localInputPipe);//working tail is the right tested value
					}
					assert(inputPosition[i]!=-1);
					assert(positionMemoData[lenIdx]<=Pipe.blobMask(localInputPipe));			
					
				} else {
		
					//System.out.println("no content on pipe "+i);
					
					TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
					
					if (trieReader.sourceLen==0 && 0==positionMemoData[stateIdx]) {  //our state is back to step 0 looking for new data
						//We have no data in the local buffer and 
						//We have no data on this pipe so go check the next one.
						
						continue;						
					} else {
						//else use the data we have since no new data came in.
						
						ccId = ccIdData[i];
						cc = (HTTPClientConnection)ccm.lookupConnectionById(ccId);					
						if (null==cc) {	//skip data the connection was closed
		
							positionMemoData[stateIdx]=0;							
							Pipe.releaseAllPendingReadLock(input[i]);
							
							TrieParserReader.parseSkip(trieReader, trieReader.sourceLen);
							TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
							continue;
						}
						
						//we have data which must be parsed and we know the output pipe, eg the connection was not closed						
						//convert long id to the pipe index.
						
						final int routeId = (int)cc.getResponsePipeIdx();
						if (routeId>=0) {
							targetPipe = output[i];
						} else {
							closeConnectionAndAbandonOldData(posIdx, lenIdx, stateIdx, cc, i);
							continue; //was closed so we can not do anything with this connection
						}				
								
						assert(checkPipeWriteState(stateIdx, targetPipe));
						
						///////////////////////
						//the fastest code is the code which is never run
						//do not parse again if nothing has changed
						////////////////////////
						final long headPos = Pipe.headPosition(localInputPipe);
						
		
						if (blockedPosition[i] == headPos && blockedLen[i] == trieReader.sourceLen) {												
							if (blockedOpenCount[i]==0 
								&& (blockedState[i] == positionMemoData[stateIdx])
								&& null!=targetPipe && Pipe.hasRoomForWrite(targetPipe) 
								) {								
								//we have the same data but we do have room for write so continue to try parse again
								blockedOpenCount[i]++;
							} else {
								if (0 == trieReader.sourceLen) {
									continue;// do not parse again since nothing has changed and we have no data	
								}
							}
						} else {
							//did not eq last so we have more data and should attempt parse again
							blockedPosition[i] = headPos;
							blockedOpenCount[i] = 0;
							blockedLen[i] = trieReader.sourceLen;
							blockedState[i] =  positionMemoData[stateIdx];							
						}						
					}
				}

	
				////////////
				////////////
				
				if (Pipe.hasContentToRead(localInputPipe) || positionMemoData[lenIdx]>localInputPipe.maxVarLen   ) {
					foundWork++;//do not leave if we are backed up
				}

			
				foundWork = parseHTTP(foundWork, i, memoIdx, posIdx, lenIdx, stateIdx, targetPipe, ccId, localInputPipe, cc);	

				 
			}
		} while(foundWork>0 && --maxIter>=0);//hasDataToParse()); //stay when very busy
	}

	private int parseHTTP(int foundWork, int i, final int memoIdx, final int posIdx, final int lenIdx,
			final int stateIdx, Pipe<NetResponseSchema> targetPipe, long ccId, Pipe<NetPayloadSchema> localInputPipe,
			HTTPClientConnection cc) {

		//case 0
		if (0 == positionMemoData[stateIdx]) {			
			if (null!=targetPipe && Pipe.hasRoomForWrite(targetPipe)) {					
				Pipe.markHead(targetPipe);
				
				int startingLength1 = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);					
				if (startingLength1 >= (revisionMap.shortestKnown()+1)) {
									
					final int revisionId = (int)TrieParserReader.parseNext(trieReader, revisionMap, -1, -2);
					if (revisionId>=0) {											
						processFoundRevision(i, stateIdx, targetPipe, ccId, cc, startingLength1);										
						assert(positionMemoData[stateIdx]==1);
					} else {
						assert(trieReader.sourceLen <= trieReader.sourceMask) : "ERROR the source length is larger than the backing array";
						TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
						
						if (-1==revisionId && (trieReader.sourceLen < (revisionMap.longestKnown()+1))) {
							foundWork = 0;//we must exit to give the other stages a chance to fix this issue
						} else {
							reportCorruptStream("HTTP revision", cc);
							closeConnectionAndAbandonOldData(posIdx, lenIdx, stateIdx, cc, i);
						}
						return foundWork;
					}
				} else {
					foundWork = 0;//we must exit to give the other stages a chance to fix this issue
					return foundWork;					
				}
			} else { 
				foundWork = 0;//we must exit to give the other stages a chance to fix this issue
				return foundWork;
			}			
		}
		////////////////////////////////////
		//case 1
		///////////////////////////////////
		if (1 == positionMemoData[stateIdx]) {
			//this writer was opened when we parsed the first line, now we are appending to it.
			DataOutputBlobWriter<NetResponseSchema> writer = Pipe.outputStream(targetPipe);
			
			final int startingPosition = writer.absolutePosition();
			long headerToken=0;
			//stay here and read all the headers if possible
			do {
				headerToken = processHeaderLogic(i, memoIdx, stateIdx, localInputPipe, cc, writer);					
			} while ((headerToken != -1) && positionMemoData[stateIdx]==1);				
			if (headerToken == -1) {
				
				writer.absolutePosition(startingPosition);
								
				if (trieReader.sourceLen<MAX_VALID_HEADER) {
					foundWork = 0;//we must exit to give the other stages a chance to fix this issue
					return foundWork;//not an error just needs more data.
				} else {
					
					//if this starts with HTTP/1.1 200 OK then we are just missing the end of the last header..
					reportCorruptStream2(cc);
					
					//TODO: bad client, disconnect??  finish partial message out!!!
					
				}
				
				TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
				
				assert(trieReader.sourceLen == Pipe.releasePendingByteCount(localInputPipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(localInputPipe);
				assert(positionMemoData[i<<2] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[i<<2]+" != "+Pipe.releasePendingByteCount(input[i]);
				return foundWork;
			}			
		}
		////////////////////////////////////
		//case 2
		///////////////////////////////////
		if (2 == positionMemoData[stateIdx]) {
			//if we can not release then do not finish.
			if (Pipe.hasRoomForWrite(releasePipe)) {
				//in case targetPipe is needed must confirm room for 2 writes .
				if (null!=targetPipe && (Pipe.hasRoomForWrite(targetPipe, SIZE_TO_WRITE))) {
					if (2 == positionMemoData[stateIdx]) {
														
						long lengthRemaining = payloadLengthData[i];
											
//						logger.info("source position {} state {} length remaining to copy {} source len ",trieReader.sourcePos,state,lengthRemaining,trieReader.sourceLen);
						
						final DataOutputBlobWriter<NetResponseSchema> writer2 = Pipe.outputStream(targetPipe);
					
						if (lengthRemaining>0 && trieReader.sourceLen>0) {
											
							//length is not written since this may accumulate and the full field provides the length
							final int consumed = TrieParserReader.parseCopy(trieReader,
									                          Math.min(lengthRemaining,
									                        		   DataOutputBlobWriter.lastBackPositionOfIndex(writer2)),
									                          writer2);
							lengthRemaining -= consumed;
							
							//NOTE: if the target field is full then we must close this one and open a new
							//      continuation.
								
							if (lengthRemaining>0) {
								DataOutputBlobWriter.commitBackData(writer2, cc.getStructureId());
																	
								int len = writer2.closeLowLevelField();
								//logger.trace("conform low level write of len {} ",len);
								Pipe.confirmLowLevelWrite(targetPipe); //uses auto size since we do not know type here
								Pipe.publishWrites(targetPipe);
								//DO NOT consume since we still need it
								
								Pipe.presumeRoomForWrite(targetPipe);
								
								//logger.trace("begin new continuation");
								
								//prep new message for next time.
								Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_CONTINUATION_102);
								Pipe.addLongValue(ccId, targetPipe); //same ccId as before
								Pipe.addIntValue(cc.sessionId, targetPipe);
								Pipe.addIntValue(0, targetPipe); //flags							
								DataOutputBlobWriter.openField(writer2);
							}								
							
//							logger.info("consumed {} source position {} state {} ",consumed, trieReader.sourcePos,state);
							
							assert(runningHeaderBytes[i]>0);
							runningHeaderBytes[i] += consumed;		
							
							TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx); 
						}
						payloadLengthData[i] = lengthRemaining;
						
						if (0 == lengthRemaining) {
//							logger.info("lenRem 0 source position {} state {} ",trieReader.sourcePos,state);
							
								
							if (Pipe.releasePendingByteCount(localInputPipe)>0) {
								Pipe.releasePendingAsReadLock(localInputPipe, runningHeaderBytes[i]); 
							}
							
							DataOutputBlobWriter.commitBackData(writer2,  cc.getStructureId());
															
							int length = writer2.closeLowLevelField(); //NetResponseSchema.MSG_RESPONSE_101_FIELD_PAYLOAD_3

							positionMemoData[stateIdx] = 5;
							
							//NOTE: go back and set the bit for end of data, 1 for msgId, 2 for connection Id	
							Pipe.orIntValue(ServerCoordinator.END_RESPONSE_MASK, 
									         targetPipe, 
									         Pipe.lastConfirmedWritePosition(targetPipe)+(0xFF&NetResponseSchema.MSG_RESPONSE_101_FIELD_CONTEXTFLAGS_5));
							
							Pipe.confirmLowLevelWrite(targetPipe, SIZE_OF_MSG_RESPONSE);
							Pipe.publishWrites(targetPipe);	
							//logger.trace("total consumed msg response write {} internal field {} varlen {} ",totalConsumed, length, targetPipe.maxVarLen);					
							//clear the usage of this pipe for use again by other connections
				
							assert (!Pipe.isInBlobFieldWrite(targetPipe)) : "for starting state expected pipe to NOT be in blob write";

							foundWork += finishAndRelease(i, 
									                        posIdx, lenIdx, stateIdx, 
									                        localInputPipe, cc, 0); 
							//positionMemoData[stateIdx] = positionMemoData[stateIdx];
														
							TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
						} else {
							foundWork = 0;//we must exit to give the other stages a chance to fix this issue
							assert(lengthRemaining>0);
							return foundWork;
						}
					}
					if (3!=positionMemoData[stateIdx]) {
						return foundWork;
					}						
				} else {
					foundWork = 0;//we must exit to give the other stages a chance to fix this issue
					return foundWork;
				}
			} else {
				foundWork = 0;//we must exit to give the other stages a chance to fix this issue
				return foundWork;					
			}
			
		}	
		//TODO: converting to conditionals, removing switches..
		
		////////////////////////////////////
		//case 3
		///////////////////////////////////
		
		 switch (positionMemoData[stateIdx]) {

			case 3: //PAYLOAD READING WITH CHUNKS	
			{
					//if we can not release then do not finish.
					if (!Pipe.hasRoomForWrite(releasePipe)) {
						foundWork = 0;//we must exit to give the other stages a chance to fix this issue
						break;
					}

			  	    long chunkRemaining = payloadLengthData[i];

					DataOutputBlobWriter<NetResponseSchema> writer3 = Pipe.outputStream(targetPipe);
					do {
						if (0==chunkRemaining) {
							
							int startingLength3 = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);	
						
							if ((int)TrieParserReader.parseNext(trieReader, HTTPUtil.chunkMap) < 0) {
								//FORMAL ERROR, we can never support a chunk bigger than a 64 bit number which is 16 chars in hex.
								if (trieReader.sourceLen>16) {
									parseErrorWhileChunking(memoIdx, localInputPipe, trieReader.sourcePos);
								}
								foundWork = 0;//we must exit to give the other stages a chance to fix this issue
								//logger.info("need chunk data");
								break;	//not enough data yet to parse try again later
							}
						
							chunkRemaining = TrieParserReader.capturedLongField(trieReader,0);
							//logger.info("reading a fresh chunk of size {}",chunkRemaining);
							
							if (0==chunkRemaining) {

								boolean foundEnd = consumeTralingHeaders(cc);										
								if (!foundEnd) {
									logger.warn("unable to find end of message");
								}
									
								TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);											
								int consumed = startingLength3 - trieReader.sourceLen;
								
								Pipe.releasePendingAsReadLock(localInputPipe, consumed);
			
								
								DataOutputBlobWriter.commitBackData(writer3, cc.getStructureId());
								
								int len = writer3.closeLowLevelField(); //NetResponseSchema.MSG_RESPONSE_101_FIELD_PAYLOAD_3
								//logger.info("nothing remaing in this chunk moving to state 5");
								positionMemoData[stateIdx] = 5;
								
								//logger.info("Detected last chunk so send the flag showing we are done\n length {}",len);
																		
								Pipe.orIntValue(ServerCoordinator.END_RESPONSE_MASK, 
										        targetPipe, 
											    Pipe.lastConfirmedWritePosition(targetPipe)+(0xFF&NetResponseSchema.MSG_RESPONSE_101_FIELD_CONTEXTFLAGS_5));
								
								Pipe.confirmLowLevelWrite(targetPipe); //uses auto size since we do not know type here
								Pipe.publishWrites(targetPipe);	
								
								//clear the usage of this pipe for use again by other connections
			
								//long routeId = cc.getResponsePipeIdx();////////WE ARE ALL DONE WITH THIS RESPONSE////////////

								//NOTE: I think this is needed but is causing a hang...
								//TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
								assert (!Pipe.isInBlobFieldWrite(targetPipe)) : "for starting state expected pipe to NOT be in blob write";
								
								foundWork += finishAndRelease(i, posIdx, lenIdx, stateIdx, localInputPipe, cc, 0); 

								break;
							} else {
									
								payloadLengthData[i] = chunkRemaining;										
								int consumed = startingLength3 - trieReader.sourceLen;
								Pipe.releasePendingAsReadLock(localInputPipe, consumed);
								TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
								
								//ensure we do not override the indexes
								if ((DataOutputBlobWriter.lastBackPositionOfIndex(writer3)-(writer3.length() + chunkRemaining))>0) {
									DataOutputBlobWriter.commitBackData(writer3, cc.getStructureId());
									
									int len = writer3.closeLowLevelField();
									//logger.trace("conform low level write of len {} ",len);
									Pipe.confirmLowLevelWrite(targetPipe); //uses auto size since we do not know type here
									Pipe.publishWrites(targetPipe);
																				
									//DO NOT consume route id we will still need it.
									//state is already 3 so leave it there
									if (Pipe.hasRoomForWrite(targetPipe)) {
										//logger.trace("begin new continuation");
										
										//prep new message for next time.
										Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_CONTINUATION_102);
										Pipe.addLongValue(ccId, targetPipe); //same ccId as before
										Pipe.addIntValue(cc.sessionId, targetPipe);
										Pipe.addIntValue(0, targetPipe); //flags
										DataOutputBlobWriter<NetResponseSchema> writer1 = Pipe.outputStream(targetPipe);							
										DataOutputBlobWriter.openField(writer1);	
										
									} else {
										//switch to 4 until the outgoing pipe is cleared
										//then we come back to 3
										positionMemoData[stateIdx] = 4;												
									}
									//logger.info("start collecting new continuation");
									
									break;
								}
							}
						}				
						
						
						////////
						//normal copy of data for chunk
						////////
						
						if (chunkRemaining>0) {
							int initValue = trieReader.sourceLen;
							//can do some but not the last byte unless we have the 2 following bytes a well. must have teh \r\n in addition to the remaining byte count								
							long maxToCopy = chunkRemaining;
							if (trieReader.sourceLen==chunkRemaining || (trieReader.sourceLen-1)==chunkRemaining) {
								maxToCopy = 0;
							}
							
						//	System.err.println("at copy postion is "+trieReader.sourcePos);
						//	TrieParserReader.debugAsUTF8(trieReader, System.err, 10, false); //we know the data is here buy why not on the stream?
							
							int temp3 = TrieParserReader.parseCopy(trieReader, maxToCopy, writer3);
							chunkRemaining -= temp3;
							
							assert(chunkRemaining>=0);
							
							if (chunkRemaining==0) {
								payloadLengthData[i] = 0; //signal that we need more, in case we exit block
								//System.err.println("ZZ copied fully over "+temp3);
								//NOTE: assert of these 2 bytes would be a good idea right here.
								int skipped = TrieParserReader.parseSkip(trieReader, 2); //skip \r\n which appears on the end of every chunk
								if (skipped!=2) {//TODO: convert to assert.
									throw new UnsupportedOperationException("Can not skip 2 bytes they are not here yet.");
								}
								temp3+=2;
							} else {
								//System.err.println("ZZ copied partial over "+temp3);
								
								payloadLengthData[i] = chunkRemaining;
							}
							TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
		
							assert((initValue-trieReader.sourceLen)==temp3);
							
							Pipe.releasePendingAsReadLock(localInputPipe, temp3);
						}
						
											
					} while (0 == chunkRemaining);
		
					break;
			}
			case 4:
			{
				//if there is no room then stay on case 4 and wait
				if (Pipe.hasRoomForWrite(targetPipe)) {
				
					//logger.trace("begin new continuation");
					
					//prep new message for next time.
					Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_CONTINUATION_102);
					Pipe.addLongValue(ccId, targetPipe); //same ccId as before
					Pipe.addIntValue(cc.sessionId, targetPipe);
					Pipe.addIntValue(0, targetPipe); //flags
					DataOutputBlobWriter<NetResponseSchema> writer1 = Pipe.outputStream(targetPipe);							
					DataOutputBlobWriter.openField(writer1);
					
					//go back and continue roll up the data
					positionMemoData[stateIdx] = 3;
					//these two lines clear the blocks so we can come back to 4 when there is data..
					blockedOpenCount[i] = 0;
					blockedState[i] = positionMemoData[stateIdx];
							
				} 
				break;
			}
			case 5: //END SEND ACK
			{
				if (Pipe.hasRoomForWrite(targetPipe)) {
					//logger.info("source position {} source length {} state {} ",trieReader.sourcePos,trieReader.sourceLen,state);
									
					assert (!Pipe.isInBlobFieldWrite(targetPipe)) : "for starting state expected pipe to NOT be in blob write";

				    foundWork += finishAndRelease(i, posIdx, lenIdx, stateIdx, localInputPipe, cc, 0);
			
				    assert(positionMemoData[(i<<2)+1] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[(i<<2)+1]+" != "+Pipe.releasePendingByteCount(input[i]);
				} else {
					foundWork = 0;
				}
				break;	
			}						
		}
		return foundWork;
	}

	private long processHeaderLogic(int i, final int memoIdx, final int stateIdx, Pipe<NetPayloadSchema> localInputPipe,
			HTTPClientConnection cc, DataOutputBlobWriter<NetResponseSchema> writer) {
		long headerToken;
		int startingLength = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);	 //TODO = save position is wrong if we continue???

		headerToken = TrieParserReader.parseNext(trieReader, cc.headerParser());	

		assert(headerToken==-1 || headerToken>=(Integer.MAX_VALUE-2)) : "bad token "+headerToken;

		int consumed = startingLength - trieReader.sourceLen;							
		runningHeaderBytes[i] += consumed;
				
		if (headerToken != -1) {											
			
			if (HTTPSpecification.END_OF_HEADER_ID != headerToken) {	
				
				headerProcessing(i, writer, headerToken, cc);
				
				//do not change state we want to come back here.									
			} else {
				endOfHeaderProcessing(i, memoIdx, stateIdx, localInputPipe, writer);																
			}
		}
		return headerToken;
	}

	private void processFoundRevision(int i, final int stateIdx, Pipe<NetResponseSchema> targetPipe, long ccId,
			HTTPClientConnection cc, int startingLength1) {
		clearConnectionStateData(i);
		
		//because we have started written the response we MUST do extra cleanup later.

		Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_RESPONSE_101);
		Pipe.addLongValue(ccId, targetPipe); // NetResponseSchema.MSG_RESPONSE_101_FIELD_CONNECTIONID_1, ccId);
		Pipe.addIntValue(cc.sessionId, targetPipe);
		Pipe.addIntValue(ServerCoordinator.BEGIN_RESPONSE_MASK, targetPipe);//flags, init to zero, will set later if required

		positionMemoData[stateIdx]++;//state change is key
							
		
		DataOutputBlobWriter<NetResponseSchema> openOutputStream = Pipe.openOutputStream(targetPipe);							
		DataOutputBlobWriter.tryClearIntBackData(openOutputStream, cc.totalSizeOfIndexes()); 
			
		if (!TrieParserReader.hasCapturedBytes(trieReader, 0)) {	
			//default ok case
			openOutputStream.writeShort(200);
		} else {
			//NOTE: this is always first and not indexed...
			TrieParserReader.writeCapturedShort(trieReader, 0, openOutputStream); //status code	
		}	
		runningHeaderBytes[i] = startingLength1 - trieReader.sourceLen;
	}

	private void endOfHeaderProcessing(int i, final int memoIdx, final int stateIdx,
			Pipe<NetPayloadSchema> localInputPipe, DataOutputBlobWriter<NetResponseSchema> writer) {
		//logger.trace("end of headers");
		positionMemoData[stateIdx] = endOfHeaderProcessing(i, stateIdx, writer);
																		
		//logger.trace("finished reading header now going to state {}",state);

		if (3==positionMemoData[stateIdx]) {
			//release all header bytes, we will do each chunk on its own.
			assert(runningHeaderBytes[i]>0);								
			Pipe.releasePendingAsReadLock(localInputPipe, runningHeaderBytes[i]); 
			runningHeaderBytes[i] = 0; 
		}
		//only case where state is not 1 so we must call save all others will call when while loops back to top.
		TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx); 

		//logger.info("payload position  {}", writer.position());
		//NOTE: payload index position is always zero 
		DataOutputBlobWriter.setIntBackData(writer, writer.position(), 0);
	}

	private boolean checkPipeWriteState(final int stateIdx, Pipe<NetResponseSchema> targetPipe) {
		if (positionMemoData[stateIdx] == 0) {
			assert (!Pipe.isInBlobFieldWrite(targetPipe)) : 
				   "for starting state expected pipe to NOT be in blob write";
		}
		return true;
	}

	private void closeConnectionAndAbandonOldData(final int posIdx, final int lenIdx, final int stateIdx, ClientConnection cc,	int i) {
		
		if (null!=cc) {	
			final int dest = cc.getResponsePipeIdx();
			
			
			//////////////////////////////////////////////////////////
			//when already closed or new closed is detected we still must clean up the in/out pipes			
			//cleanup the local pipes for both cases.			
			//////////////////////////////////////////////////////////
			if (positionMemoData[stateIdx]!=0) {
				if (dest>=0) {
					Pipe.resetHead(output[i]);
				}
				positionMemoData[stateIdx]=0;
			}
			
			assert(positionMemoData[lenIdx] == trieReader.sourceLen) : positionMemoData[lenIdx]+" vs "+trieReader.sourceLen;
			assert(positionMemoData[lenIdx]>=0);
			
			//Pipe.releasePendingAsReadLock(input[i], positionMemoData[lenIdx]);
			//assert(0== Pipe.releasePendingCount(input[i]));
			Pipe.releaseAllPendingReadLock(input[i]); //TODO: NOTE: is this right or do we need to only take those on this connection??????
			
			
			positionMemoData[posIdx] += positionMemoData[lenIdx];
			positionMemoData[lenIdx] = 0;
			assert(positionMemoData[stateIdx]==0) : "internal error";
			
			TrieParserReader.parseSkip(trieReader, trieReader.sourceLen);
			
			publishCloseAsNeeded(stateIdx, cc, i); 
		} else {
			logger.trace("connection was alread null, must already be closed");
		}
	}

	private void publishCloseAsNeeded(final int stateIdx, ClientConnection cc, int i) {
		final int dest = cc.getResponsePipeIdx();
		//if this has not been released, do so since we may be in the middle of something.
		if (inputPosition[i]>=0) {
			//release to this last point
			sendRelease(stateIdx, cc.id, inputPosition, i);
		}

		cc.clearPoolReservation(); 
		
		////////////////////////
		//send notice down stream and mark as disconnected, only done once
		////////////////////////
		
		if (!cc.isClientClosedNotificationSent()) {
			if (!cc.isDisconnecting()) {
				cc.beginDisconnect();//must be marked as disconnecting so it get re-connected if requests start up again.
			}
			
			if (dest >= 0) {
				//publish closed to notify those down stream
				Pipe<NetResponseSchema> targetPipe1 = output[i];
				Pipe.presumeRoomForWrite(targetPipe1);
				
				int size = Pipe.addMsgIdx(targetPipe1, NetResponseSchema.MSG_CLOSED_10);
				Pipe.addLongValue(cc.id, targetPipe1);
				Pipe.addIntValue(cc.sessionId, targetPipe1);
				Pipe.addUTF8(cc.host, targetPipe1);
				Pipe.addIntValue(cc.port, targetPipe1);
				Pipe.confirmLowLevelWrite(targetPipe1, size);
				Pipe.publishWrites(targetPipe1);
			
			}
			
			cc.clientClosedNotificationSent();	
			
		}
	}

	private void validateFirstByteOfHeader(final int i, final int posIdx, final int lenIdx, final int memoIdx, final int stateIdx,
			Pipe<NetPayloadSchema> localInputPipe,
			HTTPClientConnection cc, int len) {
		//we may hit zero in the middle of a payload so this check 
		//is only valid for the 0 state.
		if (len>0 && positionMemoData[stateIdx]==0) {
			//we have new data plus we know that we have no old data
			//because of this we know the first letter must be an H for HTTP.
			boolean isValid = localInputPipe.blobRing[localInputPipe.blobMask&trieReader.sourcePos]=='H';
			if (!isValid) {
				logger.warn("invalid HTTP request from server should start with H");
				if (null!=cc) {
					closeConnectionAndAbandonOldData(posIdx, lenIdx, stateIdx, cc, i);
				}
			}
		}
	}

	private boolean consumeTralingHeaders(HTTPClientConnection cc) {
		long headerToken;
		boolean foundEnd = false;
		do {
			headerToken = TrieParserReader.parseNext(trieReader, cc.headerParser());
			if (headerToken==HTTPSpecification.END_OF_HEADER_ID) {
				foundEnd = true;
				break;//MUST break here or we will read the headers of the folllowing message!!
			}
		} while (-1!=headerToken);
		return foundEnd;
	}


	private final void clearConnectionStateData(int i) {
		payloadLengthData[i] = 0;//clear payload length rules, to be populated by headers
		closeRequested[i] = false;
	}



	@Override
	public void shutdown() {
	}

	private void parseErrorWhileChunking(final int memoIdx, Pipe<NetPayloadSchema> pipe, int readingPos) {
		System.err.println("SHUTING DOWN NOW: chunk ID is TOO long starting at "+readingPos+" data remaining "+trieReader.sourceLen);
		
		ByteArrayOutputStream ist = new ByteArrayOutputStream();
		trieReader.debugAsUTF8(trieReader, new PrintStream(ist), 100,false);
		byte[] data = ist.toByteArray();
		System.err.println(new String(data));
		
		System.err.println("bad data pipe is:"+pipe);
		trieReader.debug(); //failure position is AT the mask??
		
		TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
		int chunkId3 = (int)TrieParserReader.parseNext(trieReader, HTTPUtil.chunkMap);
		System.err.println("parsed value was "+chunkId3);
		
		requestShutdown();
	}

	private void reportCorruptStream2(ClientConnection cc) {
		StringBuilder builder = new StringBuilder();
		
		trieReader.sourcePos -= 10;
		TrieParserReader.debugAsUTF8(trieReader, builder, Math.min(trieReader.sourceLen,revisionMap.longestKnown()*2));
				
		trieReader.sourcePos += 10;
		logger.warn("{} looking for header field but found:\n{}\nNOTE this starts 10 bytes before issue\n",cc.id,builder);
	}

	private void reportCorruptStream(String label, ClientConnection cc) {
		StringBuilder builder = new StringBuilder();
		TrieParserReader.debugAsUTF8(trieReader, builder, Math.min(trieReader.sourceLen,revisionMap.longestKnown()*2),false);
		logger.warn("{} looking for {} but found:\n{}\n\n",cc.id,label,builder);
	}

	private int endOfHeaderProcessing(int i, final int stateIdx, DataOutputBlobWriter<NetResponseSchema> writer) {
		int state;
		//all done with header move on to body
		writer.writeShort((short)-1); //END OF HEADER FIELDS 		
		
		//Now write header message, we know there is room because we checked before starting.
						    
		if (payloadLengthData[i] == -1) {
			positionMemoData[stateIdx]= state= 3;	
			payloadLengthData[i] = 0;//starting chunk size.			

		} else {
			positionMemoData[stateIdx]= state= 2;
		}
		return state;
	}

	private static final byte[] CLOSE = "close".getBytes();
	
	private void headerProcessing(int i, 
			                     DataOutputBlobWriter<NetResponseSchema> writer, 
			                     long headerToken, HTTPClientConnection cc) {
		
		if (HTTPSpecification.UNKNOWN_HEADER_ID == headerToken) {
			assert(confirmCoreHeadersSupported());
			boolean verboseShowHeaders = false;
			if (verboseShowHeaders) {
				reportUnsupportedHeader(); //ignore since we did not want this	
			}
		} else {
			//NOTE: any replacement headers must hold the same ordinal positions!!
			//NOTE: any header fields must hold the associated enum 
			HTTPHeader header = cc.associatedFieldObject(headerToken);
			
			//logger.info("header found {} ",header);
			
			if (null!=header) {
				int writePosition = writer.position();				

				if (HTTPHeaderDefaults.CONTENT_LENGTH.ordinal()==header.ordinal()) {
					assert(Arrays.equals(HTTPHeaderDefaults.CONTENT_LENGTH.rootBytes(),header.rootBytes())) : "Custom enums must share same ordinal positions, CONTENT_LENGTH does not match";
					
					long length = TrieParserReader.capturedLongField(trieReader, 0);
					//logger.info("content length of  {} found in headers ",length);
					
					writer.writePackedLong(length);
					
					if (-1 != payloadLengthData[i]) {
						payloadLengthData[i] = length;
												
					}			
					
				} else if (HTTPHeaderDefaults.CONTENT_TYPE.ordinal()==header.ordinal()) {
					assert(Arrays.equals(HTTPHeaderDefaults.CONTENT_TYPE.rootBytes(),header.rootBytes())) : "Custom enums must share same ordinal positions, CONTENT_TYPE does not match";
					
					int type = (int)TrieParserReader.capturedFieldQuery(trieReader, 0, trieReaderCharType, httpSpec.contentTypeTrieBuilder());
		
					//TODO: this encoding from the content type should be applied to convert the payload to the 
					//      internal standard of UTF8 if it is not already UTF8
					if (false && TrieParserReader.hasCapturedBytes(trieReaderCharType,0)) {
					   //TODO: the extracton of the encoding only does not appear to be working right.
						String encoding = trieReader.capturedFieldBytesAsUTF8(trieReaderCharType, 0, new StringBuilder()).toString();
						logger.info("encoding detected of {}",encoding);
					}
					//trieReaderCharType
					//logger.trace("detected content type and set value to {} ",type);
							
					writer.writeShort((short)type);
									
				} else if (HTTPHeaderDefaults.TRANSFER_ENCODING.ordinal()==header.ordinal()) {
					assert(Arrays.equals(HTTPHeaderDefaults.TRANSFER_ENCODING.rootBytes(),header.rootBytes())) : "Custom enums must share same ordinal positions, TRANSFER_ENCODING does not match";
								
					writer.writeBoolean(true); //true for chunked
					payloadLengthData[i] = -1; //marked as chunking	
				
				} else if (HTTPHeaderDefaults.CONNECTION.ordinal()==header.ordinal()) {
					assert(Arrays.equals(HTTPHeaderDefaults.CONNECTION.rootBytes(),header.rootBytes())) : "Custom enums must share same ordinal positions, CONNECTION does not match";
					
					//just checks for "close"
					closeRequested[i] = TrieParserReader.capturedFieldBytesEquals(trieReader, 0, CLOSE, 0, Integer.MAX_VALUE);
					
					TrieParserReader.writeCapturedValuesToDataOutput(trieReader, writer);
				} else {
					//normal processing
					TrieParserReader.writeCapturedValuesToDataOutput(trieReader, writer);
				}
				
	
				DataOutputBlobWriter.setIntBackData(writer, writePosition, StructRegistry.FIELD_MASK & (int)headerToken);
			}
			
		}
				 
	}

	private BloomFilter filter;
	
	private void reportUnsupportedHeader() {
		StringBuilder headerName = new StringBuilder();					
		TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, headerName); //in TRIE if we have any exact matches that run short must no pick anything.
		
		if (null==filter) {
			filter = new BloomFilter(10000, .00001); //32K
		}
		
		if (filter.mayContain(headerName)) {
			return;//do not report since we have already done so or we are overloaded with noise
		} else {
			filter.addValue(headerName);
		}
				
		StringBuilder headerValue = new StringBuilder();					
		TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 1, headerValue); //in TRIE if we have any exact matches that run short must no pick anything.
							
		logger.info("WARNING unsupported header found: {}: {}",headerName, headerValue);
		///logger.trace("length avail when parsed {}",len);
	}

	private boolean confirmCoreHeadersSupported() {
		StringBuilder headerName = new StringBuilder();					
		TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, headerName); //in TRIE if we have any exact matches that run short must no pick anything.
		headerName.append(": ");
		String header = headerName.toString();
				
		if (   HTTPHeaderDefaults.CONTENT_LENGTH.writingRoot().equals(header) 
			|| HTTPHeaderDefaults.CONTENT_TYPE.writingRoot().equals(header) 
			|| HTTPHeaderDefaults.TRANSFER_ENCODING.writingRoot().equals(header) 
				) {
			logger.warn("Did not recognize header {}", header);
			return false;
		}
		
		return true;
	}

	private int finishAndRelease(int i, 
								 final int posIdx, final int lenIdx, final int stateIdx, 
			                     Pipe<NetPayloadSchema> pipe, 
			                     ClientConnection cc, int nextState) {

		assert(positionMemoData[stateIdx]>=5);
		
		int foundWork = 0;
		//only ack when all the data held has been consumed.
		if (trieReader.sourceLen<=0 &&
		    Pipe.contentRemaining(pipe)==0) {	//added second rule to minimize release messages.
			
			foundWork = sendRelease(stateIdx, cc.id, inputPosition, i);						
			assert(0 == positionMemoData[stateIdx]) : "Ack must be sent to release pipe, did not happen for "+cc.id;
		} else {
			foundWork = 1;						
			positionMemoData[stateIdx] = nextState;
		}
		//records the leading edge of arrival time.
		long temp = arrivalTimeAtPosition[i];
		if (0 != temp) {
			if (cc.isValid() && !cc.isDisconnecting()) {
				cc.recordArrivalTime(temp);
			}
			arrivalTimeAtPosition[i] = 0;
		}
				
		//the server requested a close and we are now done reading the body so we need to close.
		//server side sent us "close"
		if (closeRequested[i]) {		
			
			publishCloseAsNeeded(stateIdx, cc, i);
			//close this connection but do not remove it yet.		
			cc.close();	
		}		
		
		
		return foundWork;
	}

	private int sendRelease(final int stateIdx, long ccId, long[] position, int i) {

		Pipe.presumeRoomForWrite(releasePipe);
		
		int size = Pipe.addMsgIdx(releasePipe, ReleaseSchema.MSG_RELEASE_100);
		Pipe.addLongValue(ccId, releasePipe);
		Pipe.addLongValue(position[i], releasePipe);
		Pipe.confirmLowLevelWrite(releasePipe, size);
		Pipe.publishWrites(releasePipe);
		positionMemoData[stateIdx] = 0;
		
		position[i] = -1; 
		
		return 1;

	}

}
