package com.ociweb.pronghorn.network;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeaderKey;
import com.ociweb.pronghorn.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;

import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTP1xResponseParserStage extends PronghornStage {

	private final Pipe<NetPayloadSchema>[] input; 
	private final Pipe<NetResponseSchema>[] output;
	private long[] inputPosition;
	private long[] arrivalTimeAtPosition;
	private long[] blockedPosition;
	private int[]  blockedOpenCount;
	private int[]  blockedLen;
	private int[]  blockedState;
	
	private static final boolean extractType = false;//turned off for load testing of server   TODO: this broken code is a large part of the problem for preformance !!!!!
		
	
	private final Pipe<ReleaseSchema> releasePipe;
	private final HTTPSpecification<?,?,?,?> httpSpec;
	private final int END_OF_HEADER_ID;
	private final int UNSUPPORTED_HEADER_ID;
	
	private IntHashTable listenerPipeLookup;
	private ClientCoordinator ccm;
	
	private static final Logger logger = LoggerFactory.getLogger(HTTP1xResponseParserStage.class);
	
	private static final int MAX_VALID_STATUS = 2048;
	private static final int MAX_VALID_HEADER = 32768; //much larger than most servers using 4-8K (pipe blob must be larger than this)
	
    private final static int CHUNK_SIZE = 1;
    private final static int CHUNK_SIZE_WITH_EXTENSION = 2;
   	
	private TrieParser revisionMap;
	private TrieParser headerMap;
	private TrieParser chunkMap;
	private TrieParser typeMap;
	
	
	private TrieParserReader trieReader;
	private int[] positionMemoData;
	private long[] payloadLengthData;
	private long[] ccIdData;
	
	private int[] runningHeaderBytes;
	
	
	private static final int H_TRANSFER_ENCODING = 4;	
	private static final int H_CONTENT_LENGTH = 5;
	private static final int H_CONTENT_TYPE = 6;
			  
	private int brokenDetected = 7;
	private long nextTime = System.currentTimeMillis()+20_000;
	private int lastValue = -1;
	
	
	private int[] lastMessageParseSizes;
	private int   lastMessageParseSizeCount = 0;
	
	private int lastMessageType=-1;   //does not change
	private long lastPayloadSize=-1;  //does not change
	
	public HTTP1xResponseParserStage(GraphManager graphManager, 
			                       Pipe<NetPayloadSchema>[] input, 
			                       Pipe<NetResponseSchema>[] output, Pipe<ReleaseSchema> ackStop, 
			                       IntHashTable listenerPipeLookup,
			                       ClientCoordinator ccm,
			                       HTTPSpecification<?,?,?,?> httpSpec) {
		
		super(graphManager, input, join(output,ackStop));
		this.input = input;
		this.output = output;//must be 1 for each listener
		this.ccm = ccm;
		this.releasePipe = ackStop;
		this.httpSpec = httpSpec;
		this.listenerPipeLookup = listenerPipeLookup;
		
		int i = input.length;
		while (--i>=0) {
			assert(	input[i].sizeOfBlobRing >=  MAX_VALID_HEADER*2 ); //size of blob ring is the largest a header can ever be.			
		}
		
		assert(this.httpSpec.headerMatches(H_TRANSFER_ENCODING, HTTPHeaderKeyDefaults.TRANSFER_ENCODING.getKey()));
		assert(this.httpSpec.headerMatches(H_CONTENT_LENGTH, HTTPHeaderKeyDefaults.CONTENT_LENGTH.getKey()));		
		assert(this.httpSpec.headerMatches(H_CONTENT_TYPE, HTTPHeaderKeyDefaults.CONTENT_TYPE.getKey()));
		
		this.UNSUPPORTED_HEADER_ID  = httpSpec.headerCount+2;
		this.END_OF_HEADER_ID       = httpSpec.headerCount+3;//for the empty header found at the bottom of the header
	}

	  @Override
	    public void startup() {
		  
		  lastMessageParseSizes = new int[10];
		  
	        		  
		  positionMemoData = new int[input.length<<2];
		  payloadLengthData = new long[input.length];
		  ccIdData = new long[input.length];
		  inputPosition = new long[input.length];
		  arrivalTimeAtPosition = new long[input.length];
		  blockedPosition = new long[input.length];
		  blockedOpenCount = new int[input.length];
		  blockedLen = new int[input.length];
		  blockedState = new int[input.length];
		  
		  runningHeaderBytes = new int[input.length];
		  
		  
		  trieReader = new TrieParserReader(4);//max fields we support capturing.
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
	    //  System.out.println(revisionMap.toDOT(new StringBuilder()));
	      
	      ////////////////////////
	      //Load the supported content types
	      ////////////////////////
	      
	      typeMap = new TrieParser(4096,true);	//TODO: set switch to turn on off the deep check skip     TODO: must be shared across all instances?? 
	      
	      HTTPContentType[] types = httpSpec.contentTypes;
	      x = types.length;
	      while (--x >= 0) {		
	    	//  System.err.println(types[x].contentType());
	    	  typeMap.setUTF8Value(types[x].contentType(),"\r\n", types[x].ordinal());	 
	    	  typeMap.setUTF8Value(types[x].contentType(),"\n", types[x].ordinal());  //\n must be last because we prefer to have it pick \r\n
	      }
	      //do not add extrations or byte capture they will invalidate the usage of deep check skip.
	      
	     // System.err.println("type map:"+typeMap);
	    //  System.out.println(typeMap.toDOT(new StringBuilder()));
	      
	      
	      ///////////////////////////
	      //Load the supported header keys
	      ///////////////////////////
	      boolean ignoreCase = true;
	  	  boolean supportsExtraction = true;
		  headerMap = new TrieParser(2048,1,false,supportsExtraction,ignoreCase);//deep check on to detect unexpected headers.
	 
	      HTTPHeaderKey[] shr =  httpSpec.headers;
	      x = shr.length;
	      while (--x >= 0) {
	          //must have tail because the first char of the tail is required for the stop byte
	          CharSequence key = shr[x].getKey();

	          headerMap.setUTF8Value(key, "\r\n",shr[x].ordinal());	          
	          headerMap.setUTF8Value(key, "\n",shr[x].ordinal());   //\n must be last because we prefer to have it pick \r\n
	      }    
	      headerMap.setUTF8Value("\r\n", END_OF_HEADER_ID);	        
	      headerMap.setUTF8Value("\n", END_OF_HEADER_ID);  //\n must be last because we prefer to have it pick \r\n

	      //unknowns are the least important and therefore must be added LAST to the map.
	      headerMap.setUTF8Value("%b: %b\r\n", UNSUPPORTED_HEADER_ID);	 
	      headerMap.setUTF8Value("%b: %b\n", UNSUPPORTED_HEADER_ID);  //\n must be last because we prefer to have it pick \r\n  
		      
	     // System.out.println(headerMap.toDOT(new StringBuilder()));
	      
	      
	      chunkMap = new TrieParser(128,true);
	      chunkMap.setUTF8Value("%U\r\n", CHUNK_SIZE); //hex parser of U% does not require leading 0x
	      chunkMap.setUTF8Value("%U;%b\r\n", CHUNK_SIZE_WITH_EXTENSION);
	    }

	  
	int lastUser;
	int lastState;
	int lastCount;
	
	public void storeState(int state, int user) {
		if (state==lastState && lastUser == user) {
			lastCount++;
		} else {
			lastCount = 0;
		}
		lastState = state;
		lastUser = user;
	}
	
	//TODO: when a connection is cloed here we must ALSO clear the SSL unwrap roller
	//TODO: must detected overflow and underflow parse conditions here and send error or abandon.
	
	
	int responseCount = 0;
	
	
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
				
				Pipe<NetPayloadSchema> pipe = input[i];
		
								
				/////////////////////////////////////////////////////////////
				//ensure we have the right backing array, and mask (no position change)
				/////////////////////////////////////////////////////////////
				TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
				
				TrieParserReader.parseSetup(trieReader,Pipe.blob(pipe),Pipe.blobMask(pipe));							
				
				ClientConnection cc = null;
					
				int len1 = positionMemoData[lenIdx];
				int len2 = pipe.maxAvgVarLen;
				int len3 = Pipe.blobMask(pipe);
				
				if (Pipe.hasContentToRead(pipe) 
						&& 	((positionMemoData[lenIdx]+pipe.maxAvgVarLen+Pipe.releasePendingByteCount(pipe)) < (Pipe.blobMask(pipe) ) )
						) {//MUST stay less than mask
					
					       //TOOD: this second condition above should NOT be required but under heavy load this spins and never comes back..
					//we have new data
			
					int msgIdx = Pipe.takeMsgIdx(pipe);
					if (msgIdx<0) {
						throw new UnsupportedOperationException("no support for shutdown");
					}
					
					//assert(NetPayloadSchema.MSG_PLAIN_210==msgIdx): "msgIdx "+msgIdx+"  "+pipe;
			
					ccId = Pipe.takeLong(pipe);
					
					//TODO: is this held too long??????
					long arrivalTemp = Pipe.takeLong(pipe);
					//if already set do not set again, we want the leading edge of the data arrival.
					if (arrivalTimeAtPosition[i]<=0) {
						arrivalTimeAtPosition[i] = arrivalTemp;
					}
					//TODO: how long is this waiting? is it very old?
					inputPosition[i] = Pipe.takeLong(pipe);
										
					ccIdData[i] = ccId;
					cc = (ClientConnection)ccm.get(ccId, 0);
								
					if (null==cc) {		
						logger.warn("closed connection detected");
						//abandon this record and continue
						Pipe.takeRingByteMetaData(pipe);
						Pipe.takeRingByteLen(pipe);
						Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx)); 		
						Pipe.releaseReadLock(pipe);
						positionMemoData[memoIdx+1] = 0;//wipe out existing data
						
						//drain all the data on this pipe
						Pipe.publishBlobWorkingTailPosition(pipe, Pipe.getBlobWorkingHeadPosition(pipe));
						Pipe.publishWorkingTailPosition(pipe, Pipe.workingHeadPosition(pipe));
						
						//let go of pipe
						ccm.releaseResponsePipeLineIdx(ccId);
												
						continue;
					}
				
					targetPipe = output[(short)IntHashTable.getItem(listenerPipeLookup, cc.getUserId())];					
	
					//append the new data
					int meta = Pipe.takeRingByteMetaData(pipe);
					int len = Pipe.takeRingByteLen(pipe);
					int pos = Pipe.bytePosition(meta, pipe, len);
					int mask = Pipe.blobMask(pipe);
					
					//logger.info("parse new data of {} for connection {}",len,cc.getId());
	
					if (positionMemoData[lenIdx]==0) {
						positionMemoData[posIdx] = pos;						
						positionMemoData[lenIdx] = len;
					} else {				
						positionMemoData[lenIdx] += len;
					}
					
					assert(positionMemoData[lenIdx]<Pipe.blobMask(pipe)) : "error adding "+len+" total was "+positionMemoData[lenIdx]+" and should be < "+pipe.blobMask(pipe);
					

					TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
					
					//System.err.println(positionMemoData[lenIdx]+" vs "+pipe.byteMask);
					
				//	logger.info("reading in new data up to "+Pipe.getWorkingTailPosition(pipe)+" has "+positionMemoData[lenIdx]+" mask "+Pipe.blobMask(pipe));
					
					Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));   //release of read does not happen until the bytes are consumed...
					
					//WARNING: moving next without releasing lock prevents new data from arriving until after we have consumed everything.
					//  
					Pipe.readNextWithoutReleasingReadLock(pipe);	
							
					if (-1==inputPosition[i]) {
						//this may or may not be the end of a complete message, must hold it just in case it is.
						inputPosition[i] = Pipe.getWorkingTailPosition(pipe);//working tail is the right tested value
					}
					assert(inputPosition[i]!=-1);
					
					if 	(positionMemoData[lenIdx] >= Pipe.blobMask(pipe) ) {
						logger.info("NEW CORRUPT DATA ERROR, response is not keeping up with the data "+positionMemoData[lenIdx]+" added "+len+" which should be < "+pipe.maxAvgVarLen);
						logger.info("{}  {}  {}",len1,len2,len3);
						logger.info("FORCE EXIT");
						System.exit(-1);
					}
					
					
				} else {
		
					
					TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
					
					if (trieReader.sourceLen==0 &&        //we have no old data
						0==positionMemoData[stateIdx]) {  //our state is back to step 0 looking for new data
						//We have no data in the local buffer and 
						//We have no data on this pipe so go check the next one.
			
						continue;
						
					} else {
						//else use the data we have since no new data came in.
						
						ccId = ccIdData[i];
						cc = (ClientConnection)ccm.get(ccId, 0);					
						if (null==cc) {	//skip data the connection was closed		
							TrieParserReader.parseSkip(trieReader, trieReader.sourceLen);
							TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
							logger.info("not done, still must reset all resources like the rollers");
							continue;
						}
						
						//we have data which must be parsed and we know the output pipe, eg the connection was not closed						
						//convert long id to the pipe index.
						targetPipe = output[(short)IntHashTable.getItem(listenerPipeLookup, cc.getUserId())]; 		
							
						///////////////////////
						//the fastest code is the code which is never run
						//do not parse again if nothing has changed
						////////////////////////
						final long headPos = Pipe.headPosition(pipe);
						
						//TODO: this same approach should be used in server
						if (blockedPosition[i] == headPos && blockedLen[i] == trieReader.sourceLen) {
					
							if (blockedOpenCount[i]==0 && Pipe.hasRoomForWrite(targetPipe) && blockedState[i] == positionMemoData[stateIdx] ) {								
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
					
//					if 	((positionMemoData[lenIdx]) >= Pipe.blobMask(pipe) ) {
//						logger.info("OLD CORRUPT DATA ERROR, response is not keeping up with the data "+positionMemoData[lenIdx]);
//						logger.info("FORCE EXIT");
//						System.exit(-1);
//					}
				}
	
			
				
				if (Pipe.hasContentToRead(pipe) || positionMemoData[lenIdx]>pipe.maxAvgVarLen   ) {
					foundWork++;//do not leave if we are backed up
				}

				
				int state = positionMemoData[stateIdx];

				if (state==0) { //TODO: are two sharing the same pipe?
					assert (!Pipe.isInBlobFieldWrite(targetPipe)) : "for starting state expected pipe to NOT be in blob write";
				}
				
				

			//	logger.info("before test position {} state {} ",trieReader.sourcePos,state);
				
				//because the messages come in different sized headers this can not be used to test the NETTY.
				boolean testingMode = true; //not working with chunked trailing byte
				
				//TODO: must support multiple file requests, responses are in order? so we can provide this with a script fileA, fileB fileC then repeat.
				//      periodic confirm file and each file has seen header lengths to check.
				
				//TODO: only turn on feature after first 1000 or so normal requests with checks
				
				//almost always take this if when testing.
				final int mask = 0xFFFFF;
				if (((++responseCount&mask)!=0) && testingMode) {
					int lastMessageParseSize = -1;
					int p = lastMessageParseSizeCount;
					while (--p>=0) {
						int size = lastMessageParseSizes[p];
						byte b= pipe.blobRing[trieReader.sourceMask&(trieReader.sourcePos+size-1)];
					
						if (b==10) {
							lastMessageParseSize = size;
							break;
						}
						
					}				
				
					if (0==state && testingMode && lastMessageParseSize>0 && trieReader.sourceLen>=lastMessageParseSize  && Pipe.hasRoomForWrite(targetPipe)) {
						//This is a cheat to make the client go faster so we have the required speed to test the server
											
						TrieParserReader.parseSkip(trieReader, (int)(lastMessageParseSize-lastPayloadSize));
						Pipe.releasePendingAsReadLock(pipe, lastMessageParseSize);
						
						
						//because we have started writign the response we MUST do extra cleanup later.
						Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_RESPONSE_101);
						Pipe.addLongValue(ccId, targetPipe); // NetResponseSchema.MSG_RESPONSE_101_FIELD_CONNECTIONID_1, ccId);
						
						
						DataOutputBlobWriter<NetResponseSchema> writer = Pipe.outputStream(targetPipe);
						try {
						DataOutputBlobWriter.openField(writer);
						} catch (Exception e) {
							System.err.println("open write for "+cc.id+" and user "+cc.getUserId());
							
							throw e;
						}
						
						writer.writeShort(200);//OK
						
						writer.writeShort((short)H_CONTENT_TYPE);
						writer.writeShort((short)lastMessageType); //JSONType					
						writer.writeShort((short)-1); //END OF HEADER FIELDS
					    TrieParserReader.parseCopy(trieReader, (int)lastPayloadSize, writer);
						
						
						writer.closeLowLevelField(); //NetResponseSchema.MSG_RESPONSE_101_FIELD_PAYLOAD_3
						Pipe.confirmLowLevelWrite(targetPipe, Pipe.sizeOf(NetResponseSchema.instance, NetResponseSchema.MSG_RESPONSE_101));
						Pipe.publishWrites(targetPipe);	
									
	
						positionMemoData[stateIdx] = state = 5;
						foundWork += finishAndRelease(i, stateIdx, pipe, cc); 
						
						TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
						
						//TODO: must tests 1 fraction of the requests.
						
						
						continue;
					}
				
				}
				
				
				int initial = -1;
				
				
				//TOOD: may be faster with if rather than switch.
				
	//			logger.info("ZZ source position {} state {} len {} ",trieReader.sourcePos,state, trieReader.sourceLen);
				
				 switch (state) {
					case 0:////HTTP/1.1 200 OK              FIRST LINE REVISION AND STATUS NUMBER
						initial = trieReader.sourcePos;
						if (null==targetPipe || !Pipe.hasRoomForWrite(targetPipe)) { 
			//				logger.info("break A");
							break; //critical check
						}
						
						int startingLength1 = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
						
						if (startingLength1<(revisionMap.shortestKnown()+1)) {
			//				logger.info("break B");
							break;
						}						
					
						final int revisionId = (int)TrieParserReader.parseNext(trieReader, revisionMap);
						if (revisionId>=0) {
							
							payloadLengthData[i] = 0;//clear payload length rules, to be populated by headers
							
							{
								//because we have started writign the response we MUST do extra cleanup later.
								Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_RESPONSE_101);
								Pipe.addLongValue(ccId, targetPipe); // NetResponseSchema.MSG_RESPONSE_101_FIELD_CONNECTIONID_1, ccId);
								
								//TODO: this is a serious client parse problem, we CAN NOT pick up new connection ID until this one is finished!!!
								//      breaks on multi user since we hold this open...  TODO: must be fixed by reservation and the ClientSocketReader.....
								
								DataOutputBlobWriter<NetResponseSchema> writer1 = Pipe.outputStream(targetPipe);							
								DataOutputBlobWriter.openField(writer1);							
								TrieParserReader.writeCapturedShort(trieReader, 0, writer1); //status code	
							}
							
							positionMemoData[stateIdx]= ++state;
							
							int consumed = startingLength1 - trieReader.sourceLen;						
							
							runningHeaderBytes[i] = consumed;
			
						} else {
							
							TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
							
							if (trieReader.sourceLen<(revisionMap.longestKnown()+1)) {
				//				logger.info("break C");
								break;//not an error just needs more data.
							} else {
								//TODO: rollback the previous message write since it can not be compeleted? or just trucate it?? TODO: urgent error support
								
								reportCorruptStream("HTTP revision",cc);
								
//								trieReader.sourcePos-=100;
//								trieReader.sourceLen+=100;
//								TrieParserReader.debugAsUTF8(trieReader, System.out, trieReader.sourceLen, false);
//								trieReader.sourcePos+=100;
//								trieReader.sourceLen-=100;
								
								///////////////////////////////////
								//server is behaving badly so shut the connection
								//////////////////////////////////
								cc.close();		
								cc.clearPoolReservation();
								ccm.releaseResponsePipeLineIdx(cc.id);
								
								TrieParserReader.parseSkip(trieReader, trieReader.sourceLen);
								TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
								
								
							}
			//				logger.info("break D");
							break;
						}
						
						assert(positionMemoData[stateIdx]==1);
					case 1: ///////// HEADERS
						//this writer was opened when we parsed the first line, now we are appending to it.
						DataOutputBlobWriter<NetResponseSchema> writer = Pipe.outputStream(targetPipe);
						
						int headerId=0;
						//stay here and read all the headers if possible
						do {
							int startingLength = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);	 //TODO = save position is wrong if we continue???
												
														
							int len = trieReader.sourceLen;
							headerId = (int)TrieParserReader.parseNext(trieReader, headerMap);	
						
									
							if (headerId>=0) {					
								
								if (END_OF_HEADER_ID != headerId) {
									//only some headers are supported the rest are ignored									
									specialHeaderProcessing(i, writer, headerId, len);									
									//do not change state we want to come back here.									
								} else {
									state = endOfHeaderProcessing(i, stateIdx, writer);		

									if (3==state) {
										//release all header bytes, we will do each chunk on its own.
										assert(runningHeaderBytes[i]>0);								
										Pipe.releasePendingAsReadLock(pipe, runningHeaderBytes[i]); 
										runningHeaderBytes[i] = 0;
									}
									//only case where state is not 1 so we must call save all others will call when while loops back to top.
									TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx); //TODO = save position is wrong if we continue???
								}
								
								
								int consumed = startingLength - trieReader.sourceLen;
							
							//	assert(runningHeaderBytes[i]>0);
								runningHeaderBytes[i] += consumed;
								
							} 
						} while (headerId>=0 && state==1);
						
						if (headerId<0) {
							TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
							
							if (trieReader.sourceLen<MAX_VALID_HEADER) {
		//						logger.info("break E");
								break;//not an error just needs more data.
							} else {
								reportCorruptStream2(cc);
								
								//TODO: bad client, disconnect??  finish partial message out!!!
								
							}
							
							assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
							assert(positionMemoData[i<<2] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[i<<2]+" != "+Pipe.releasePendingByteCount(input[i]);
				    		break;
						}
						
				
					case 2: //PAYLOAD READING WITH LENGTH
							if (2==state) {
								
								
								long lengthRemaining = payloadLengthData[i];
																	
		//						logger.info("source position {} state {} length remaining to copy {} source len ",trieReader.sourcePos,state,lengthRemaining,trieReader.sourceLen);
								
								final DataOutputBlobWriter<NetResponseSchema> writer2 = Pipe.outputStream(targetPipe);
								
								if (lengthRemaining>0 && trieReader.sourceLen>0) {
				
									//logger.info("** payload reading with length");
									final int consumed = TrieParserReader.parseCopy(trieReader, lengthRemaining, writer2);
									lengthRemaining -= consumed;
							
		//							logger.info("consumed {} source position {} state {} ",consumed, trieReader.sourcePos,state);
									
									assert(runningHeaderBytes[i]>0);
									runningHeaderBytes[i] += consumed;		
									
									TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx); //TODO = save position is wrong if we continue???
								}
								payloadLengthData[i] = lengthRemaining;
								
								if (0 == lengthRemaining) {
		//							logger.info("lenRem 0 source position {} state {} ",trieReader.sourcePos,state);
									
									Pipe.releasePendingAsReadLock(pipe, runningHeaderBytes[i]); 
									
									//NOTE: input is low level, TireParser is using low level take
									//      writer output is high level;									
									writer2.closeLowLevelField(); //NetResponseSchema.MSG_RESPONSE_101_FIELD_PAYLOAD_3
									positionMemoData[stateIdx] = state = 5;
									Pipe.confirmLowLevelWrite(targetPipe, Pipe.sizeOf(NetResponseSchema.instance, NetResponseSchema.MSG_RESPONSE_101));
									Pipe.publishWrites(targetPipe);	
														
									assert(trieReader.sourceLen<=0 || input[i].blobRing[input[i].blobMask&trieReader.sourcePos]=='H') :"bad next value of "+(int)input[i].blobRing[input[i].blobMask&trieReader.sourcePos];
									
									foundWork += finishAndRelease(i, stateIdx, pipe, cc); 
									state = positionMemoData[stateIdx];
									
									if (initial>=0) {

										lastMessageParseSize(trieReader.sourcePos-initial);

									}
									
									TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
									
									//should only go up?? and start at low position??
							//	    logger.info("finished read at source position {} state {} instance {} len {} mask {}",trieReader.sourcePos,state,stageId, trieReader.sourceLen, trieReader.sourceMask);
																			
									break;
								} else {
									
		//							logger.info("needs len {}",lengthRemaining);
									
									assert(lengthRemaining>0);
	//								logger.info("break F");
									break;//we have no data and need more.
								}
							}
							if (3!=state) {
								break;
							}
	
					case 3: //PAYLOAD READING WITH CHUNKS	
						
	//					logger.info("source position {} state {} ",trieReader.sourcePos,state);
	///					
			//			    assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
							long chunkRemaining = payloadLengthData[i];
							DataOutputBlobWriter<NetResponseSchema> writer3 = Pipe.outputStream(targetPipe);
							do {
								
								logger.info("****************************  chunk remainining {} ",chunkRemaining);
								
								if (0==chunkRemaining) {
									
	//								assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
									int startingLength3 = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);	
										
//									System.out.print("NEW BLOCK TEXT: ");
//									trieReader.debugAsUTF8(trieReader, System.out, 100,false);
//									System.out.println();
									
									int chunkId = (int)TrieParserReader.parseNext(trieReader, chunkMap);
							
									if (chunkId < 0) {
										
										//restore position so we can debug.
										TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
										
										int readingPos = trieReader.sourcePos;
										
										if (trieReader.sourceLen>16) { //FORMAL ERROR
											System.err.println("chunk ID is TOO long starting at "+readingPos+" data remaining "+trieReader.sourceLen);
											
											ByteArrayOutputStream ist = new ByteArrayOutputStream();
											trieReader.debugAsUTF8(trieReader, new PrintStream(ist), 100,false);
											byte[] data = ist.toByteArray();
											System.err.println(new String(data));
											
											System.err.println("bad data pipe is:"+pipe);
											trieReader.debug(); //failure position is AT the mask??
											
											TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
											int chunkId3 = (int)TrieParserReader.parseNext(trieReader, chunkMap);
											System.err.println("parsed value was "+chunkId3);
											
											requestShutdown();											
										}
																			
			//							assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe)+" pos "+trieReader.sourcePos+"  "+pipe;
										
										return;	//not enough data yet to parse try again later
									}					
							
									chunkRemaining = TrieParserReader.capturedLongField(trieReader,0);
									
									//logger.info("*** parsing new HTTP payload of size {}",chunkRemaining);
									
									if (0==chunkRemaining) {
										
										//TODO: Must add parse support for trailing headers!, this is a hack for now.
										headerId = (int)TrieParserReader.parseNext(trieReader, headerMap);
										TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
											
										int consumed = startingLength3 - trieReader.sourceLen;
					
										Pipe.releasePendingAsReadLock(pipe, consumed);
										assert(END_OF_HEADER_ID==headerId);
										
										//NOTE: input is low level, TireParser is using low level take
										//      writer output is high level;									
										int len = writer3.closeLowLevelField(); //NetResponseSchema.MSG_RESPONSE_101_FIELD_PAYLOAD_3
										positionMemoData[stateIdx] = state = 5;
										Pipe.confirmLowLevelWrite(targetPipe, Pipe.sizeOf(NetResponseSchema.instance, NetResponseSchema.MSG_RESPONSE_101));
										Pipe.publishWrites(targetPipe);	
							
										foundWork += finishAndRelease(i, stateIdx, pipe, cc); 
										
										if (initial>=0) {
											lastMessageParseSize(trieReader.sourcePos-initial);
										}
										
	//									assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe)+" pos "+trieReader.sourcePos+"  "+pipe;
										
										
										break;
									} else {
										
										int consumed = startingLength3 - trieReader.sourceLen;
								
										Pipe.releasePendingAsReadLock(pipe, consumed);
										
//										assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
										TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
									}
								}				
								
								
								////////
								//normal copy of data for chunk
								////////
								
								int temp3 = TrieParserReader.parseCopy(trieReader, chunkRemaining, writer3);
//								if (temp3>=0) {
//									foundWork = true;
//								}
				//					System.out.println("    chunk removed total of "+temp3);
								chunkRemaining -= temp3;
								
								assert(chunkRemaining>=0);
								
								if (chunkRemaining==0) {
									//NOTE: assert of these 2 bytes would be a good idea right here.
									TrieParserReader.parseSkip(trieReader, 2); //skip \r\n which appears on the end of every chunk
									temp3+=2;
								}
								
//								assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
								TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
			
								Pipe.releasePendingAsReadLock(pipe, temp3);
													
							} while (0 == chunkRemaining);
							
							payloadLengthData[i] = chunkRemaining;
							
		//					assert(positionMemoData[i<<2] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[i<<2]+" != "+Pipe.releasePendingByteCount(input[i]);
							
							break;
					
					case 5: //END SEND ACK
						logger.info("source position {} state {} ",trieReader.sourcePos,state);
						
					    foundWork += finishAndRelease(i, stateIdx, pipe, cc);
						if (initial>=0) {
						    lastMessageParseSize(trieReader.sourcePos-initial);
						}
					    assert(positionMemoData[(i<<2)+1] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[(i<<2)+1]+" != "+Pipe.releasePendingByteCount(input[i]);
					    
						break;						
				}	
				 
				 
		//		 TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
				 
				 
		//		 logger.info("after case source position {} state {} ",trieReader.sourcePos,state);
				 
			}
		} while(foundWork>0);//hasDataToParse()); //stay when very busy

		//TODO: is work on pipes?
		
	}

	private void lastMessageParseSize(int size) {
		
		int i = lastMessageParseSizeCount;
		while (--i>=0) {
			if (size == lastMessageParseSizes[i]) {
				return; //nothing to do
			}
		}		
		lastMessageParseSizes[lastMessageParseSizeCount++] = size;		
	}

	

//	private boolean hasDataToParse() {
//		int i = input.length;
//	
//		while (--i>=0) {
//		
//				final int memoIdx = i<<2; //base index is i  * 4;
//				//final int posIdx   = memoIdx;
//				final int lenIdx   = memoIdx+1;
//				//final int stateIdx = memoIdx+2;		
//				if (positionMemoData[lenIdx]>200_000) {
//					return true;
//				}
//		
//		}
//		return false;
//	}

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
		//TODO: in the future should use multiple fragments to allow for streaming response, important feature.
		
		//logger.info("**************** end of headers length values is {}",payloadLengthData[i]);
										    
		if (payloadLengthData[i]<0) {
			positionMemoData[stateIdx]= state= 3;	
			payloadLengthData[i] = 0;//starting chunk size.			

		} else {
			positionMemoData[stateIdx]= state= 2;	
			assert(payloadLengthData[i]>3) : "only support files of non zero length at this time, still testing. found value "+payloadLengthData[i];
		}
		return state;
	}

	private void specialHeaderProcessing(int i, DataOutputBlobWriter<NetResponseSchema> writer, int headerId, int len) {
		switch (headerId) {
			case H_TRANSFER_ENCODING:
			
				payloadLengthData[i] = -1; //marked as chunking										
				break;
			case H_CONTENT_LENGTH:										
				long length = TrieParserReader.capturedLongField(trieReader, 0);
				if (-1 != payloadLengthData[i]) {
					payloadLengthData[i] = length;
					lastPayloadSize = length;
					
	//				logger.info("at position {}, we found the length {}  ", trieReader.sourcePos, length);
											
				}
				break;
				
			//other values to write to stream?	
			case H_CONTENT_TYPE:										
				writer.writeShort((short)H_CONTENT_TYPE);
				
				if (extractType) {				
					
					//TODO: another way to do this is to merge the typeMap into the header map on constuction, this should improve performance by doing single pass.
					int oldPos = trieReader.sourcePos;
					int oldLen = trieReader.sourceLen;
					int type = (int)TrieParserReader.capturedFieldQuery(trieReader, 0, typeMap);
					lastMessageType = type;
					if (type<0) { 
	//					StringBuilder captured = new StringBuilder();
	//					TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, captured);
	//					logger.info("Unable to recognize content type {} ",captured); //TODO: this is still broken why??
					}				
					writer.writeShort((short)type);
	
					trieReader.sourceLen = oldLen;
					trieReader.sourcePos = oldPos; //NOTE: should we have to put this back??
	//				if (oldPos!=trieReader.sourcePos) {
	//					throw new RuntimeException();
	//				}
					//positionMemoData[stateIdx]
																
				} else {
					
//					StringBuilder temp = new StringBuilder();
//					TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, temp);
//					logger.info("{} A {}   captured length: {}",trieReader.sourcePos,httpSpec.headers[headerId], temp.length());
					
					writer.writeShort(lastMessageType = -1);
				}
				
				break;
			default:
				if (headerId ==UNSUPPORTED_HEADER_ID) {
					reportUnsupportedHeader(len);					
				} else {

					
//					StringBuilder temp = new StringBuilder();
//					TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, temp);
//					logger.info("{} {} B {}   captured length: {}",headerId,trieReader.sourcePos,httpSpec.headers[headerId], temp.length());
	
				}
				break;
		}
				 
	}

	private void reportUnsupportedHeader(int len) {
		StringBuilder headerName = new StringBuilder();					
		TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, headerName); //in TRIE if we have any exact matches that run short must no pick anything.
		
		StringBuilder headerValue = new StringBuilder();					
		TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 1, headerValue); //in TRIE if we have any exact matches that run short must no pick anything.
							
		logger.info("WARNING unsupported header found: {}: {}",headerName, headerValue);
		logger.info("length avail when parsed {}",len);
	}

	private int finishAndRelease(int i, final int stateIdx,
			Pipe<NetPayloadSchema> pipe, ClientConnection cc) {
		
		assert(5==positionMemoData[stateIdx]);
		int foundWork = 0;
		
		//only ack when all the data held has been consumed.
		if (trieReader.sourceLen<=0 &&
		    Pipe.contentRemaining(pipe)==0) {	//added second rule to minimize release messages.
			//TODO: if not sent we should stay on ack step.
			
			foundWork = sendRelease(stateIdx, cc.id, inputPosition, i);
			//may return without setting ack because pipe is full.	
			if (positionMemoData[stateIdx] != 0) {
				logger.info("not finished {})",cc.id);
			}

		} else {
			foundWork = 1;						
			positionMemoData[stateIdx] = 0; //next state	
			cc.recordArrivalTime(arrivalTimeAtPosition[i]);
			arrivalTimeAtPosition[i] = 0;
		}
		
		if (ServerCoordinator.TEST_RECORDS &&  trieReader.sourceLen>0) {
		   char ch = (char)pipe.blobRing[ pipe.blobMask&trieReader.sourcePos ];
		   if ('H'!=ch) {
			   //this error was not fixed why is length so long?
			   //[HTTP1xResponseParserStage id:30] INFO com.ociweb.pronghorn.network.HTTP1xResponseParserStage - FORCE EXIT FOUND BAD MESSAGE BEGINNING FOUND 
			   //BUT SHOULD BE H, LENGTH IS 16401533
			   logger.info("WHEN DONE, FORCE EXIT FOUND BAD MESSAGE BEGINNING FOUND {} BUT SHOULD BE H, LENGTH IS {} blobMask {} ",ch,trieReader.sourceLen, pipe.blobMask);
			   new Exception("must step over last return").printStackTrace();
			   System.exit(-1);
			   
		   }
			
			
		}
		
		
		return foundWork;
	}

	private int sendRelease(final int stateIdx, long ccId, long[] position, int i) {

		if (!Pipe.hasRoomForWrite(releasePipe)) {
			logger.info("warning, must send release or client may hang, pipe was backed up so we must wait");
			Pipe.spinBlockForRoom(releasePipe, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASE_100));
		}
		
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
