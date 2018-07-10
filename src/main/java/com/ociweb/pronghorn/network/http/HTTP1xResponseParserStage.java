package com.ociweb.pronghorn.network.http;

import static com.ociweb.pronghorn.pipe.Pipe.blobMask;
import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;

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
	private static final int SIZE_OF_MSG_RESPONSE = Pipe.sizeOf(NetResponseSchema.instance, NetResponseSchema.MSG_RESPONSE_101);
	private final Pipe<NetPayloadSchema>[] input; 
	private final Pipe<NetResponseSchema>[] output;
	private int[] outputOwner; //tracking active use of the output
	
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
	
	private static final int MAX_VALID_STATUS = 2048;
	private static final int MAX_VALID_HEADER = 32768; //much larger than most servers using 4-8K (pipe blob must be larger than this)

   	
	private TrieParser revisionMap;
	
	private TrieParserReader trieReader;
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
		this.output = output;//must be 1 for each listener
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
		
	}

	  @Override
	    public void startup() {
		  
		  outputOwner = new int[output.length];
		  Arrays.fill(outputOwner, -1);
		  
	        		  
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
		  			  
		  trieReader = new TrieParserReader();//max fields we support capturing.
		  
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
	    	   //TODO: since most responses are 200 would this run faster if we hard coded that as part of the tree??
	            revisionMap.setUTF8Value(revs[x].getKey(), " %u %b\r\n", revs[x].ordinal());
	            revisionMap.setUTF8Value(revs[x].getKey(), " %u %b\n", revs[x].ordinal());    //\n must be last because we prefer to have it pick \r\n
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
		
		int foundWork; //keep going until we make a pass and there is no work.
			

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
					ccId = Pipe.peekLong(localInputPipe, 1);
					boolean alsoReturnDisconnected = true;
					cc = (HTTPClientConnection)ccm.connectionForSessionId(ccId, alsoReturnDisconnected);
				
					if (null != cc) {
						//do not process if an active write for a different pipe is in process
						if (     (i != outputOwner[(int)cc.readDestinationRouteId()]) 
								&&  (-1 != outputOwner[(int)cc.readDestinationRouteId()])) {
							//move to the next pipe so we get it done.
							continue;
						}
						outputOwner[(int)cc.readDestinationRouteId()] = i;
					}

					//////////////////////////////
					//we have new data to consume
					//////////////////////////////		
					final int msgIdx = Pipe.takeMsgIdx(localInputPipe);
					final int sizeOf = Pipe.sizeOf(localInputPipe, msgIdx);
					if (msgIdx>=0) {//does not read the body if this is a shutdown request.
											
						//is closed
						if (((null==cc) || (!cc.isValid()))) {	
					
							logger.info("closed {} connection detected ",cc);
							if (null != cc) {
								
								//publish closed to notify those down stream
								Pipe<NetResponseSchema> targetPipe1 = output[(int)cc.readDestinationRouteId()];
								
								publishCloseMessage(cc.host, cc.port, targetPipe1);
							}
							positionMemoData[lenIdx] = 0;//wipe out existing data
							positionMemoData[stateIdx] = 0;
							
							Pipe.skipNextFragment(localInputPipe, msgIdx);							
							continue;
						}
										
						boolean ok = ccId == Pipe.takeLong(localInputPipe);
						assert(ok) : "Internal error";
						
						long arrivalTime = Pipe.takeLong(localInputPipe);
						//if already set do not set again, we want the leading edge of the data arrival.
						if (arrivalTimeAtPosition[i]<=0) {
							arrivalTimeAtPosition[i] = arrivalTime;
						}
						
						inputPosition[i] = Pipe.takeLong(localInputPipe);										
						ccIdData[i] = ccId;
			
					
						targetPipe = output[(int)cc.readDestinationRouteId()];					
		
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
							
							//we may hit zero in the middle of a payload so this check 
							//is only valid for the 0 state.
							if (len>0 && positionMemoData[stateIdx]==0) {
								//we have new data plus we know that we have no old data
								//because of this we know the first letter must be an H for HTTP.
								boolean isValid = localInputPipe.blobRing[localInputPipe.blobMask&trieReader.sourcePos]=='H';
								if (!isValid) {
									logger.warn("invalid HTTP request from server should start with H");
									if (null!=cc) {
										badServerSoCloseConnection(memoIdx, cc);
										//publish closed to notify those down stream
										Pipe<NetResponseSchema> targetPipe1 = output[(int)cc.readDestinationRouteId()];
										
										publishCloseMessage(cc.host, cc.port, targetPipe1);
									}
								}
							}
							
						} else {				
							positionMemoData[lenIdx] += len;
						}
						
						assert(positionMemoData[lenIdx]<Pipe.blobMask(localInputPipe)) : "error adding "+len+" total was "+positionMemoData[lenIdx]+" and should be < "+localInputPipe.blobMask(localInputPipe);
	
						TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
	
						
					}
					
					
					
					//done consuming this message.
					
					Pipe.confirmLowLevelRead(localInputPipe, sizeOf);   //release of read does not happen until the bytes are consumed...
					
					//WARNING: moving next without releasing lock prevents new data from arriving until after we have consumed everything.
					//  
					Pipe.readNextWithoutReleasingReadLock(localInputPipe);	
														
					
					if (-1==inputPosition[i]) {
						//this may or may not be the end of a complete message, must hold it just in case it is.
						inputPosition[i] = Pipe.getWorkingTailPosition(localInputPipe);//working tail is the right tested value
					}
					assert(inputPosition[i]!=-1);
					assert(positionMemoData[lenIdx]<=Pipe.blobMask(localInputPipe));			
					
				} else {
		
					
					TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
					
					if (trieReader.sourceLen==0 &&        //we have no old data
						0==positionMemoData[stateIdx]) {  //our state is back to step 0 looking for new data
						//We have no data in the local buffer and 
						//We have no data on this pipe so go check the next one.
						if (Pipe.contentRemaining(localInputPipe)>(localInputPipe.slabMask*.75)) {
							logger.warn("Can not read content because old data has not been released.");
							logger.warn("exited");
							System.exit(-1);
						}
						continue;
						
					} else {
						//else use the data we have since no new data came in.
						
						ccId = ccIdData[i];
						cc = (HTTPClientConnection)ccm.connectionForSessionId(ccId);					
						if (null==cc) {	//skip data the connection was closed	
							ccm.releaseResponsePipeLineIdx(ccId);
							
							TrieParserReader.parseSkip(trieReader, trieReader.sourceLen);
							TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
					
							continue;
						}
						
						//we have data which must be parsed and we know the output pipe, eg the connection was not closed						
						//convert long id to the pipe index.
						
						targetPipe = output[(int)cc.readDestinationRouteId()];
												
												
						///////////////////////
						//the fastest code is the code which is never run
						//do not parse again if nothing has changed
						////////////////////////
						final long headPos = Pipe.headPosition(localInputPipe);
						
						//TODO: this same approach should be used in server
						if (blockedPosition[i] == headPos 
							&& blockedLen[i] == trieReader.sourceLen) {
												
							if (blockedOpenCount[i]==0 
								&& Pipe.hasRoomForWrite(targetPipe) 
								&& blockedState[i] == positionMemoData[stateIdx] ) {								
								//we have the same data but we do have room for write so continue to try parse again
								//blockedPosition[i] = 0;	
								blockedOpenCount[i]++;
							} else {
							
								continue;// do not parse again since nothing has changed	
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
				//do not process if an active write is in process
				if (null!=cc) {
					if (     (i != outputOwner[(int)cc.readDestinationRouteId()]) 
					    &&  (-1 != outputOwner[(int)cc.readDestinationRouteId()])) {
						//multiple connections write to the same pipe, this keeps them organized.
						//move to the next one because this one is blocked
						//System.err.println("qqq");
						continue;
					}
					outputOwner[(int)cc.readDestinationRouteId()] = i;
				}
				////////////
				////////////
				
				if (Pipe.hasContentToRead(localInputPipe) || positionMemoData[lenIdx]>localInputPipe.maxVarLen   ) {
					foundWork++;//do not leave if we are backed up
				}

				int state = positionMemoData[stateIdx];

				if (state==0) {
					assert (!Pipe.isInBlobFieldWrite(targetPipe)) : 
						   "for starting state expected pipe to NOT be in blob write";
				}
			
					//TODO: may be faster with if rather than switch.

				//System.err.println("on state "+state);
				
				 switch (state) {
					case 0:////HTTP/1.1 200 OK              FIRST LINE REVISION AND STATUS NUMBER
						
						if (null==targetPipe || !Pipe.hasRoomForWrite(targetPipe)) { 
							break; //critical check
						}
						
						int startingLength1 = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
						
						if (startingLength1<(revisionMap.shortestKnown()+1)) {
							break;
						}		
					
						final int revisionId = (int)TrieParserReader.parseNext(trieReader, revisionMap);
						if (revisionId>=0) {
													
							clearConnectionStateData(i);
							
							//because we have started written the response we MUST do extra cleanup later.
							Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_RESPONSE_101);
							Pipe.addLongValue(ccId, targetPipe); // NetResponseSchema.MSG_RESPONSE_101_FIELD_CONNECTIONID_1, ccId);
						
							Pipe.addIntValue(ServerCoordinator.BEGIN_RESPONSE_MASK, targetPipe);//flags, init to zero, will set later if required

							positionMemoData[stateIdx]= ++state;//state change is key
							DataOutputBlobWriter<NetResponseSchema> openOutputStream = Pipe.openOutputStream(targetPipe);
							
							DataOutputBlobWriter.tryClearIntBackData(openOutputStream, cc.totalSizeOfIndexes()); 
	
							//NOTE: this is always first and not indexed...
							TrieParserReader.writeCapturedShort(trieReader, 0, openOutputStream); //status code	
										
							runningHeaderBytes[i] = startingLength1 - trieReader.sourceLen;
	
												
						} else {
							assert(trieReader.sourceLen <= trieReader.sourceMask) : "ERROR the source length is larger than the backing array";
							TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
							
							if (trieReader.sourceLen < (revisionMap.longestKnown()+1)) {
								break;//not an error just needs more data.
							} else {
								//TODO: rollback the previous message write since it can not be compeleted? or just trucate it?? TODO: urgent error support
								
								//logger.info("error trieReader pos {} len {} ", trieReader.sourcePos,trieReader.sourceLen);
								
								reportCorruptStream("HTTP revision",cc);

								badServerSoCloseConnection(memoIdx, cc);
																
							}
		
							break;
						}
						
						assert(positionMemoData[stateIdx]==1);
					case 1: ///////// HEADERS
						
						//TODO: look up the right headerMap...
						//      these are based on the headers that the client caller requests
						//      these headers should be defined in the ClientHostPortInstance object.
						
						
						//this writer was opened when we parsed the first line, now we are appending to it.
						DataOutputBlobWriter<NetResponseSchema> writer = Pipe.outputStream(targetPipe);
						
						final int startingPosition = writer.absolutePosition();
							
						
						long headerToken=0;
						//stay here and read all the headers if possible
						do {
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
									//logger.trace("end of headers");
									state = endOfHeaderProcessing(i, stateIdx, writer);
																									
									//logger.trace("finished reading header now going to state {}",state);
								
									if (3==state) {
										//release all header bytes, we will do each chunk on its own.
										assert(runningHeaderBytes[i]>0);								
										Pipe.releasePendingAsReadLock(localInputPipe, runningHeaderBytes[i]); 
										runningHeaderBytes[i] = 0; 
									}
									//only case where state is not 1 so we must call save all others will call when while loops back to top.
									TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx); 

									//logger.info("payload position {} {}  {}",Long.toBinaryString(cc.payloadToken),cc.payloadToken,writer.position());
									//NOTE: payload index position is always zero 
									DataOutputBlobWriter.setIntBackData(writer, writer.position(), 0);
																		
								}
							} 
							
						} while ((headerToken != -1) && state==1);
						
						if (headerToken == -1) {
							
							writer.absolutePosition(startingPosition);
							
							TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
							
							if (trieReader.sourceLen<MAX_VALID_HEADER) {		
								break;//not an error just needs more data.
							} else {
							    
								reportCorruptStream2(cc);
								
								//TODO: bad client, disconnect??  finish partial message out!!!
								
							}
							
							assert(trieReader.sourceLen == Pipe.releasePendingByteCount(localInputPipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(localInputPipe);
							assert(positionMemoData[i<<2] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[i<<2]+" != "+Pipe.releasePendingByteCount(input[i]);
				    		break;
						}
						
				
					case 2: //PAYLOAD READING WITH LENGTH
							//if we can not release then do not finish.
							if (!Pipe.hasRoomForWrite(releasePipe)) {
								break;
							}
							//in case targetPipe is needed must confirm room for 2 writes .
							if (!Pipe.hasRoomForWrite(targetPipe, 2*Pipe.sizeOf(targetPipe, NetResponseSchema.MSG_CONTINUATION_102))) {
								break;
							}
							if (2==state) {
																
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
									
										
									Pipe.releasePendingAsReadLock(localInputPipe, runningHeaderBytes[i]); 
														
									DataOutputBlobWriter.commitBackData(writer2,  cc.getStructureId());
																	
									int length = writer2.closeLowLevelField(); //NetResponseSchema.MSG_RESPONSE_101_FIELD_PAYLOAD_3
									//logger.info("length of full message written {} ",length);
									
									positionMemoData[stateIdx] = state = 5;
									
									//NOTE: go back and set the bit for end of data, 1 for msgId, 2 for connection Id	
									Pipe.orIntValue(ServerCoordinator.END_RESPONSE_MASK, 
											         targetPipe, 
											         Pipe.lastConfirmedWritePosition(targetPipe)+(0xFF&NetResponseSchema.MSG_RESPONSE_101_FIELD_CONTEXTFLAGS_5));
									
									Pipe.confirmLowLevelWrite(targetPipe, SIZE_OF_MSG_RESPONSE);
									int totalConsumed = Pipe.publishWrites(targetPipe);	
									//logger.trace("total consumed msg response write {} internal field {} varlen {} ",totalConsumed, length, targetPipe.maxVarLen);					
									//clear the usage of this pipe for use again by other connections
									outputOwner[(int)cc.readDestinationRouteId()] = -1; 
									long routeId = cc.consumeDestinationRouteId();////////WE ARE ALL DONE WITH THIS RESPONSE////////////

									//NOTE: I think this is needed but is causing a hang...
									//TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
									//if (Pipe.hasRoomForWrite(targetPipe)) {
									
										assert (!Pipe.isInBlobFieldWrite(targetPipe)) : "for starting state expected pipe to NOT be in blob write";
	
										foundWork += finishAndRelease(i, stateIdx, localInputPipe, cc, 0, targetPipe); 
										state = positionMemoData[stateIdx];
																	
										TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
										if (0==state) {
											validateNextByte(i, memoIdx, cc);
										}
									//} else {
									//	foundWork = 0;
									//}
									break;
								} else {
									
									assert(lengthRemaining>0);
									break;//we have no data and need more.
								}
							}
							if (3!=state) {
								break;
							}
	
					case 3: //PAYLOAD READING WITH CHUNKS	
						
							//if we can not release then do not finish.
							if (!Pipe.hasRoomForWrite(releasePipe)) {
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
										//logger.info("need chunk data");
										return;	//not enough data yet to parse try again later
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
										positionMemoData[stateIdx] = state = 5;
										
										//logger.info("Detected last chunk so send the flag showing we are done\n length {}",len);
																				
										Pipe.orIntValue(ServerCoordinator.END_RESPONSE_MASK, 
												        targetPipe, 
													    Pipe.lastConfirmedWritePosition(targetPipe)+(0xFF&NetResponseSchema.MSG_RESPONSE_101_FIELD_CONTEXTFLAGS_5));
										
										Pipe.confirmLowLevelWrite(targetPipe); //uses auto size since we do not know type here
										Pipe.publishWrites(targetPipe);	
										
										//clear the usage of this pipe for use again by other connections
										outputOwner[(int)cc.readDestinationRouteId()] = -1; 
										long routeId = cc.consumeDestinationRouteId();////////WE ARE ALL DONE WITH THIS RESPONSE////////////

										//NOTE: I think this is needed but is causing a hang...
										//TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
										//if (Pipe.hasRoomForWrite(targetPipe)) {
											assert (!Pipe.isInBlobFieldWrite(targetPipe)) : "for starting state expected pipe to NOT be in blob write";
	
					                    	foundWork += finishAndRelease(i, stateIdx, localInputPipe, cc, 0, targetPipe); 
										//} else {
										//	foundWork = 0;
										//}
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
												Pipe.addIntValue(0, targetPipe); //flags
												DataOutputBlobWriter<NetResponseSchema> writer1 = Pipe.outputStream(targetPipe);							
												DataOutputBlobWriter.openField(writer1);	
												
											} else {
												//switch to 4 until the outgoing pipe is cleared
												//then we come back to 3
												positionMemoData[stateIdx] = state = 4;												
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
					case 4:
						//if there is no room then stay on case 4 and wait
						if (Pipe.hasRoomForWrite(targetPipe)) {
						
							//logger.trace("begin new continuation");
							
							//prep new message for next time.
							Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_CONTINUATION_102);
							Pipe.addLongValue(ccId, targetPipe); //same ccId as before
							Pipe.addIntValue(0, targetPipe); //flags
							DataOutputBlobWriter<NetResponseSchema> writer1 = Pipe.outputStream(targetPipe);							
							DataOutputBlobWriter.openField(writer1);
							
							//go back and continue roll up the data
							positionMemoData[stateIdx] = state = 3;
							//these two lines clear the blocks so we can come back to 4 when there is data..
							blockedOpenCount[i] = 0;
							blockedState[i] = positionMemoData[stateIdx];
									
						} 
						break;
					case 5: //END SEND ACK
						if (Pipe.hasRoomForWrite(targetPipe)) {
							//logger.info("source position {} source length {} state {} ",trieReader.sourcePos,trieReader.sourceLen,state);
											
							assert (!Pipe.isInBlobFieldWrite(targetPipe)) : "for starting state expected pipe to NOT be in blob write";
	
						    foundWork += finishAndRelease(i, stateIdx, localInputPipe, cc, 0, targetPipe);
					
						    assert(positionMemoData[(i<<2)+1] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[(i<<2)+1]+" != "+Pipe.releasePendingByteCount(input[i]);
						} else {
							foundWork = 0;
						}
						break;	
												
				}	

				 
			}
		} while(foundWork>0);//hasDataToParse()); //stay when very busy
		
	}

	private void validateNextByte(int i, final int memoIdx, HTTPClientConnection cc) {
		/////////////////////////////////////////////////////
		/////////////////////////////////////////////////////
		
		//expecting H to be the next valid char in the buffer 
		boolean isPipeValid = trieReader.sourceLen<=0 
			|| input[i].blobRing[input[i].blobMask&trieReader.sourcePos]=='H';
		
		//server has sent data which is not HTTP request
		//directly after this request
		if (!isPipeValid) {
			logger.warn("server has sent unparsable data '{}' after a complete message.",
					(char)input[i].blobRing[input[i].blobMask&trieReader.sourcePos]);
			//close this bad server...
			badServerSoCloseConnection(memoIdx, cc);
		}
	}

	private void badServerSoCloseConnection(final int memoIdx, HTTPClientConnection cc) {
		///////////////////////////////////
		//server is behaving badly so shut the connection
		//////////////////////////////////
		cc.close();		
		cc.clearPoolReservation();
		ccm.releaseResponsePipeLineIdx(cc.id);
		
		TrieParserReader.parseSkip(trieReader, trieReader.sourceLen);
		TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
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

	private void publishCloseMessage(CharSequence host, int port, Pipe<NetResponseSchema> targetPipe) {
		
		Pipe.presumeRoomForWrite(targetPipe);
		
		int size = Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_CLOSED_10);
		Pipe.addUTF8(host, targetPipe);
		Pipe.addIntValue(port, targetPipe);
		Pipe.confirmLowLevelWrite(targetPipe, size);
		Pipe.publishWrites(targetPipe);
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
		TrieParserReader.debugAsUTF8(trieReader, builder, revisionMap.longestKnown()*2);
		logger.warn("{} looking for header field but found:\n{}\n\n",cc.id,builder);
	}

	private void reportCorruptStream(String label, ClientConnection cc) {
		StringBuilder builder = new StringBuilder();
		TrieParserReader.debugAsUTF8(trieReader, builder, revisionMap.longestKnown()*2,false);
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
								
					int type = (int)TrieParserReader.capturedFieldQuery(trieReader, 0, httpSpec.contentTypeTrieBuilder());
	
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

	private int finishAndRelease(int i, final int stateIdx, 
			                     Pipe<NetPayloadSchema> pipe, 
			                     ClientConnection cc, int nextState,
			                     Pipe<NetResponseSchema> targetPipe) {

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
			cc.recordArrivalTime(temp);
			arrivalTimeAtPosition[i] = 0;
		}
				
		//the server requested a close and we are now done reading the body so we need to close.
		if (closeRequested[i]  //server side sent us "close"
		   //DO NOT look at cc for the state since we have multiple messages in flight
		    ) {
			//logger.info("Client got close request so do it and push down stream");				
			publishCloseMessage(cc.host, cc.port, targetPipe);
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
