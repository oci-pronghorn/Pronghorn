package com.ociweb.pronghorn.network;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.HdrHistogram.EncodableHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeaderKey;
import com.ociweb.pronghorn.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTP1xResponseParserStage extends PronghornStage {

	private final Pipe<NetPayloadSchema>[] input; 
	private final Pipe<NetResponseSchema>[] output;
	private long[] inputPosition;
	private long[] blockedPosition;
	private int[]  blockedOpenCount;
	private int[]  blockedLen;
	private int[]  blockedState;
	
	
	
	private final Pipe<ReleaseSchema> ackStop;
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
	
	
	private static final int H_TRANSFER_ENCODING = 4;	
	private static final int H_CONTENT_LENGTH = 5;
	private static final int H_CONTENT_TYPE = 6;
		
	
	private int runLength = 0;	  
	private int brokenDetected = 7;
	private long nextTime = System.currentTimeMillis()+20_000;
	private int lastValue = -1;
	
	int[] totalBytes;
	  	  
	
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
		this.ackStop = ackStop;
		this.httpSpec = httpSpec;
		this.listenerPipeLookup = listenerPipeLookup;
		//this.supportsBatchedRelease = false;
		this.totalBytes = new int[input.length];
		
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
	        		  
		  positionMemoData = new int[input.length<<2];
		  payloadLengthData = new long[input.length];
		  ccIdData = new long[input.length];
		  inputPosition = new long[input.length];
		  blockedPosition = new long[input.length];
		  blockedOpenCount = new int[input.length];
		  blockedLen = new int[input.length];
		  blockedState = new int[input.length];
		  
		  trieReader = new TrieParserReader(4);//max fields we support capturing.
		  int x;
		  
		  //HTTP/1.1 200 OK
		  //revision  status# statusString\r\n
		  //headers
		  
	      ///////////////////////////
	      //Load the supported HTTP revisions
	      ///////////////////////////
	      revisionMap = new TrieParser(256,false); //avoid deep check
	      HTTPRevision[] revs = httpSpec.supportedHTTPRevisions.getEnumConstants();
	      x = revs.length;               
	      while (--x >= 0) {
	            int b = revisionMap.setUTF8Value(revs[x].getKey(), " %u %b\n", revs[x].ordinal());   
	            revisionMap.setUTF8Value(revs[x].getKey(), " %u %b\r\n", revs[x].ordinal());
	      }
	      
	      ////////////////////////
	      //Load the supported content types
	      ////////////////////////
	      
	      typeMap = new TrieParser(4096,false);	      
	      
	      HTTPContentType[] types = httpSpec.contentTypes;
	      x = types.length;
	      while (--x >= 0) {	    	  
	    	  typeMap.setUTF8Value(types[x].contentType(),"\r\n", types[x].ordinal());	    	  
	      }
	      typeMap.setUTF8Value("%b\r\n", 0);
    	  
	      
	      
	      ///////////////////////////
	      //Load the supported header keys
	      ///////////////////////////
	      headerMap = new TrieParser(2048,false);//deep check on to detect unexpected headers.
	      HTTPHeaderKey[] shr =  httpSpec.headers;
	      x = shr.length;
	      while (--x >= 0) {
	          //must have tail because the first char of the tail is required for the stop byte
	          CharSequence key = shr[x].getKey();
	          
	          if (H_CONTENT_TYPE == x) {
	        	  assert(key.charAt(key.length()-2)=='%');
	        	  assert(key.charAt(key.length()-1)=='b');
	        	  key = key.subSequence(0, key.length()-2);//removes %b from the end so we can us a seprate parser just to determine the content type.
	        	  	        	  
	        	  headerMap.setUTF8Value(key,shr[x].ordinal());	        	  
	        	  
	          } else {	        	  
	        	  headerMap.setUTF8Value(key, "\n",shr[x].ordinal());  //TODO: urgent the insert of this conditional breaks on the wrong point
	        	  headerMap.setUTF8Value(key, "\r\n",shr[x].ordinal());
	          }
	      }
	      headerMap.setUTF8Value("%b: %b\n", UNSUPPORTED_HEADER_ID);  //TODO: urgent the insert of this conditional breaks on the wrong point
	      headerMap.setUTF8Value("%b: %b\r\n", UNSUPPORTED_HEADER_ID);	  //TODO: urgent the insert of this conditional breaks on the wrong point
	      
	      headerMap.setUTF8Value("\n", END_OF_HEADER_ID); //Detecting this first but not right!! we did not close the revision??
	      headerMap.setUTF8Value("\r\n", END_OF_HEADER_ID);	        
 	      
	      chunkMap = new TrieParser(128,false);
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
	
	
	  
	public void addBytes(int len, Pipe pipe, int i, int sourceLen) {
		assert(len>0) : "bad len "+len;
		totalBytes[i]+=len;
		assert(totalBytes[i] == Pipe.releasePendingByteCount(pipe)) : totalBytes[i]+" != "+Pipe.releasePendingByteCount(pipe); 
		assert(totalBytes[i] == sourceLen) : totalBytes[i]+" != "+sourceLen;
		//logger.info("expected {} actual {} pos {} {}",totalBytes[i], Pipe.releasePendingByteCount(pipe), Pipe.tailPosition(pipe), i);
		//logger.info("{} vs {} ",totalBytes[i],sourceLen);
		
	}
	
	public void subBytes(int len, Pipe pipe, int i, int sourceLen) {
		assert(len>=0) : "bad len "+len;
		totalBytes[i]-=len;
		assert(totalBytes[i] == Pipe.releasePendingByteCount(pipe)) : totalBytes[i]+" != "+Pipe.releasePendingByteCount(pipe);
		assert(totalBytes[i] == sourceLen) : totalBytes[i]+" != "+sourceLen;
		//logger.info("expected {} actual {} pos {} {} ",totalBytes[i], Pipe.releasePendingByteCount(pipe), Pipe.tailPosition(pipe), i);
		///logger.info("{} vs {} ",totalBytes[i],sourceLen);
		
	}
	
	@Override
	public void run() {
		
		boolean foundWork; //keep going until we make a pass and there is no work.
	
		do {		
			foundWork = false;;
			
			int i = input.length;
			while (--i>=0) {
								
				final int memoIdx = i<<2; //base index is i  * 4;
				final int posIdx   = memoIdx;
				final int lenIdx   = memoIdx+1;
				final int stateIdx = memoIdx+2;
				
				Pipe<NetResponseSchema> targetPipe = null;
				long ccId = 0;
				
				Pipe<NetPayloadSchema> pipe = input[i];
	
		        assert(recordIncomingState(!Pipe.hasContentToRead(pipe)));		       
				assert(positionMemoData[(i<<2)+1] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[(i<<2)+1]+" != "+Pipe.releasePendingByteCount(input[i]);
				assert(totalBytes[i] == Pipe.releasePendingByteCount(pipe)) : totalBytes[i]+" != "+Pipe.releasePendingByteCount(pipe); 
								
				/////////////////////////////////////////////////////////////
				//ensure we have the right backing array, and mask (no position change)
				/////////////////////////////////////////////////////////////
				TrieParserReader.parseSetup(trieReader,Pipe.blob(pipe),Pipe.blobMask(pipe));							
				
				ClientConnection cc = null;
				
				if (!Pipe.hasContentToRead(pipe)) {
				
					TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
					
					assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe)+" pos "+trieReader.sourcePos+"  "+pipe;
					
					if (trieReader.sourceLen==0 &&        //we have no old data
						0==positionMemoData[stateIdx]) {  //our state is back to step 0 looking for new data
						//We have no data in the local buffer and 
						//We have no data on this pipe so go check the next one.
						
						assert(0==Pipe.releasePendingByteCount(pipe)) : "pending data to release of "+Pipe.releasePendingByteCount(pipe)+" yet sourceLen is zero???";
						assert(0==totalBytes[i]) : "error total bytes "+totalBytes[i];
						
						continue;
						
					} else {
						//else use the data we have since no new data came in.
						
						ccId = ccIdData[i];
						cc = (ClientConnection)ccm.get(ccId, 0);					
						if (null==cc) {	//skip data the connection was closed		
							TrieParserReader.parseSkip(trieReader, trieReader.sourceLen);
							TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
							logger.info("must reset all resources");
							continue;
						}
						
						//we have data which must be parsed and we know the output pipe, eg the connection was not closed				
							
						
						//assert(0==cc.getUserId() || IntHashTable.hasItem(listenerPipeLookup, cc.getUserId())) : "no value found for "+cc.getUserId();
						
						targetPipe = output[(short)IntHashTable.getItem(listenerPipeLookup, cc.getUserId())]; 		
						
						assert(recordOutgoingState(!Pipe.hasRoomForWrite(targetPipe)));
						 
						assert (0!=positionMemoData[stateIdx] || !Pipe.isInBlobFieldWrite(targetPipe)) : "for starting state expected pipe to NOT be in blob write";
												
						///////////////////////
						//the fastest code is the code which is never run
						//do not parse again if nothing has changed
						////////////////////////
						final long headPos = Pipe.headPosition(pipe);
						
						//TODO: this same approach should be used in server
						if (blockedPosition[i] == headPos && blockedLen[i] == trieReader.sourceLen) {
					
							if (blockedOpenCount[i]==0 && PipeWriter.hasRoomForWrite(targetPipe) && blockedState[i] == positionMemoData[stateIdx] ) {								
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
							foundWork = true;
						}
						
					}
				} else {	
					//we have new data
					
					assert(positionMemoData[lenIdx] == Pipe.releasePendingByteCount(pipe)) : positionMemoData[lenIdx]+" != "+Pipe.releasePendingByteCount(pipe);
										
					
					int msgIdx = Pipe.takeMsgIdx(pipe);
					//assert(NetPayloadSchema.MSG_PLAIN_210==msgIdx): "msgIdx "+msgIdx+"  "+pipe;
			
					ccId = Pipe.takeLong(pipe);
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
						continue;
					}
					foundWork = true;//new data found to consume, add it to the trie parser
					
					//assert(0==cc.getUserId() || IntHashTable.hasItem(listenerPipeLookup, cc.getUserId())) : "no value found for "+cc.getUserId();
										
					targetPipe = output[(short)IntHashTable.getItem(listenerPipeLookup, cc.getUserId())];					
					assert (0!=positionMemoData[stateIdx] || !Pipe.isInBlobFieldWrite(targetPipe)) : "for starting state expected pipe to NOT be in blob write";
					
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
					
					assert(positionMemoData[lenIdx]<pipe.blobMask(pipe)) : "error "+len+" should be < "+pipe.blobMask(pipe);
					

					TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
										
					
					//System.err.println(positionMemoData[lenIdx]+" vs "+pipe.byteMask);
					
					
					Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));   //release of read does not happen until the bytes are consumed...
					
					//WARNING: moving next without releasing lock prevents new data from arriving until after we have consumed everything.
					//  
					Pipe.readNextWithoutReleasingReadLock(pipe);	
					
					addBytes(len, pipe, i, trieReader.sourceLen);
					assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
		
					if (-1==inputPosition[i]) {
						//this may or may not be the end of a complete message, must hold it just in case it is.
						inputPosition[i] = Pipe.getWorkingTailPosition(pipe);//working tail is the right tested value
					}
					assert(inputPosition[i]!=-1);
				}
	
				int state = positionMemoData[stateIdx];

//				storeState(state,cc.getUserId());
//				boolean debugHang = (lastCount>4000);
//				if (debugHang) {
//					logger.info("ERROR: parse state {} from pipe {} for connection {} frozen and missing data {} {}",state,i,cc.id,trieReader.sourceLen,pipe); //TODO: this is not an error?? just needs to wait long?
//		
//					logger.info("Pipe release count: {} byte {} ", Pipe.releasePendingCount(pipe), Pipe.releasePendingByteCount(pipe));
//										
//					//cc.close();
//					//ccm.releaseResponsePipeLineIdx(cc.id);
//					
//					System.exit(-1);
//					//return;
//				}
				
				
				//TODO: only try to parse this pipe again if it has new additional data,  add this key feature to save cycles.
				
				
				
				switch (state) {
					case 0:////HTTP/1.1 200 OK              FIRST LINE REVISION AND STATUS NUMBER
						if (!PipeWriter.hasRoomForWrite(targetPipe)) {
							break;
						}
						int startingLength1 = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
						runLength = 0;
						boolean isLongEnough = trieReader.sourceLen>=MAX_VALID_HEADER;
						final int revisionId = (int)TrieParserReader.parseNext(trieReader, revisionMap);
						if (revisionId>=0) {
							
							payloadLengthData[i] = 0;//clear payload length rules, to be populated by headers
							
							{
								DataOutputBlobWriter<NetResponseSchema> writer = PipeWriter.outputStream(targetPipe);							
								writer.openField();							
								TrieParserReader.writeCapturedShort(trieReader, 0, writer); //status code
								foundWork = true;
								
							}
							
							positionMemoData[stateIdx]= ++state;
							
							int consumed = startingLength1 - trieReader.sourceLen;
							runLength += consumed;
							Pipe.releasePendingAsReadLock(pipe, consumed);
							subBytes(consumed, pipe, i, trieReader.sourceLen);
							
//							if (pipe.contentRemaining(pipe)>0  &&  !Pipe.hasContentToRead(pipe)) {
//								throw new RuntimeException("no match");	
//							}
							
						} else {
							
							TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
							
//							if (trieReader.sourceLen>=5) { //TODO: modfy this to check each byte and catch problems sooner.
//								StringBuilder temp = new StringBuilder();
//								try{
//									trieReader.debugAsUTF8(trieReader, temp, 5, false);
//								} catch (Throwable t) {
//									logger.warn("error parsing",t);
//									throw new RuntimeException("Corrupt data at start of new HTTP message", t);
//								}
//								String t = temp.toString().trim();
//								if (t.endsWith("...")) {
//									t = t.substring(0, t.length()-3);
//								}
//								if (t.length()>0 && !"HTTP/".equals(t)) {
//									logger.warn("bad data found at {} in {} masks {} {} ",trieReader.sourcePos, pipe, Pipe.slabMask(pipe), Pipe.blobMask(pipe));
//									 
//											
//									throw new RuntimeException("Corrupt data at start of new HTTP message. len :"+t.length()+" found :"+t);
//								}					
//							}
							
							
//							if (isLongEnough) { //IF BIGGER THAN MAX THIS IS AN ERROR???
//								
//								ClientConnection connection = ccm.get(ccId);
//								if (null!=connection) {
//									//server is behaving badly so we do not bother with handshake just close this NOW.
//									connection.close();
//								}
//								
////								//TODO: send a better message we can recover from !!!!!!
////								
////								TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
////								
////								logger.warn("is long enough, bad data from server can not parse {} bytes to find revision",trieReader.sourceLen);
////								
////								ByteArrayOutputStream ist = new ByteArrayOutputStream();
////								trieReader.debugAsUTF8(trieReader, new PrintStream(ist), MAX_VALID_STATUS, false);
////								logger.warn("'"+new String(ist.toByteArray())+"'");
////								
////								requestShutdown();
//								return;
//								
//							}
							
							assert(positionMemoData[i<<2] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[i<<2]+" != "+Pipe.releasePendingByteCount(input[i]);
							break;
						}
						
						assert(positionMemoData[stateIdx]==1);
					case 1: ///////// HEADERS
						//this writer was opened when we parsed the first line, now we are appending to it.
						DataOutputBlobWriter<NetResponseSchema> writer = PipeWriter.outputStream(targetPipe);
						
						boolean foundEnd = false;
						do {
							assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
							int startingLength = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);	
							boolean isTooLarge = trieReader.sourceLen>=MAX_VALID_HEADER;							
							
							//does not MOVE the position
//							boolean showHeaders = true;
//							if (showHeaders) {
//								TrieParserReader.debugAsUTF8(trieReader, System.out);
//							}

							
							int headerId = (int)TrieParserReader.parseNext(trieReader, headerMap);
							if (headerId>=0) {
								
								foundWork = true;
								if (END_OF_HEADER_ID != headerId) {
									
//									if (headerId == UNSUPPORTED_HEADER_ID) {
//										logger.info("WARNING unsupported header found");
//									} else {
//										logger.info("found header {} {}",headerId,this.httpSpec.headers[headerId]);
//									}
									
									//only some headers are supported the rest are ignored
									
									switch (headerId) {
										case H_TRANSFER_ENCODING:
											//logger.info("xxxxxxxxxx chunked ");
											payloadLengthData[i] = -1; //marked as chunking										
											break;
										case H_CONTENT_LENGTH:										
											long length = TrieParserReader.capturedLongField(trieReader, 0);
											//logger.info("**********************  Captured length field was {} ",length);
											if (-1 != payloadLengthData[i]) {
												payloadLengthData[i] = length;
											}
											break;
											
										//other values to write to stream?	
										case H_CONTENT_TYPE:										
											writer.writeShort((short)H_CONTENT_TYPE);
											
											int type = (int)TrieParserReader.parseNext(trieReader, typeMap);
											
											//logger.info("wrote out content type of {} {}",type,this.httpSpec.contentTypes[type]);
											
											
											writer.writeShort((short)type);
											break;
																											
									}
									
									//do not change state we want to come back here.
									
								} else {									
									
									//all done with header move on to body
									writer.writeShort((short)-1); //END OF HEADER FIELDS 		
									
									//Now write header message, we know there is room because we checked before starting.
									//TODO: in the future should use multiple fragments to allow for streaming response, important feature.
									PipeWriter.tryWriteFragment(targetPipe, NetResponseSchema.MSG_RESPONSE_101);
								    PipeWriter.writeLong(targetPipe, NetResponseSchema.MSG_RESPONSE_101_FIELD_CONNECTIONID_1, ccId);
	
								    //logger.info("**************** end of headers length values is {}",payloadLengthData[i]);
								    								    
									if (payloadLengthData[i]<0) {
										positionMemoData[stateIdx]= state= 3;	
										payloadLengthData[i] = 0;//starting chunk size.
									} else {
										positionMemoData[stateIdx]= state= 2;									
									}
									foundEnd = true;
									
								}
								TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
								
								int consumed = startingLength - trieReader.sourceLen;
								runLength += consumed;
								Pipe.releasePendingAsReadLock(pipe, consumed);
								assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
								subBytes(consumed, pipe, i, trieReader.sourceLen);

								assert(positionMemoData[(i<<2)+1] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[(i<<2)+1]+" != "+Pipe.releasePendingByteCount(input[i]);
								
							} else {
								TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
								
//								if (isTooLarge) {
//									//this is bigger than the acceptable header so the server must have sent something bad
//									
//									ClientConnection connection = (ClientConnection)ccm.get(ccId, 0);
//									if (null!=connection) {
//										//server is behaving badly so we do not bother with handshake just close this NOW.
//										connection.close();
//									}
//									
//									//TODO: send a better message we can recover from !!!!!!
//									
//									TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
//									assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
//									
//									logger.warn("is too large, bad data from server can not parse {} bytes to find revision",trieReader.sourceLen);
//									
//								//	ByteArrayOutputStream ist = new ByteArrayOutputStream();
//								//	trieReader.debugAsUTF8(trieReader, new PrintStream(ist), MAX_VALID_STATUS, false); is not always UTF8 so this just distracts from the true error.
//								//	logger.warn("'"+new String(ist.toByteArray())+"'");
//									
//									requestShutdown();
//									
//									logger.info("RETURN FORM TOO LARGE");
//									
//									return;									
//									
//								}
								
								assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
								//could not parse, we need more content, 
								//we will detect the overload error when we fetch new data not here.
								//continue after we get more data.
								
								assert(positionMemoData[i<<2] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[i<<2]+" != "+Pipe.releasePendingByteCount(input[i]);
					    		break;
							}
						} while(!foundEnd);
						assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
						
					case 2: //PAYLOAD READING WITH LENGTH
							if (2==state) {
								//TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
								
								//logger.info("** payload reading with length");
								long lengthRemaining = payloadLengthData[i];
								DataOutputBlobWriter<NetResponseSchema> writer2 = PipeWriter.outputStream(targetPipe);
						
//								if (debugHang) {
//									logger.info("copy bytes of the required {} from {} at pos {} pipe {} ",lengthRemaining,trieReader.sourceLen, (trieReader.sourcePos& pipe.byteMask), pipe);
//								}
								
								int consumed = TrieParserReader.parseCopy(trieReader, lengthRemaining, writer2);
								
								if (consumed>=0) {								
									foundWork = true;
								}
								
								
							//	logger.info("payload consumed {}",consumed);
								
							//	System.err.println("length rem "+lengthRemaining+"  "+temp);
								
								lengthRemaining -= consumed;
								runLength += (consumed);
								Pipe.releasePendingAsReadLock(pipe, consumed); 
								subBytes(consumed, pipe, i, trieReader.sourceLen);
//								if (pipe.contentRemaining(pipe)>0  &&  !Pipe.hasContentToRead(pipe)) {
//									throw new RuntimeException("no match");	
//								}
								
								
								payloadLengthData[i] = lengthRemaining;
								assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
								TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
								
								
								
								if (0 == lengthRemaining) {
									//NOTE: input is low level, TireParser is using low level take
									//      writer output is high level;									
									writer2.closeHighLevelField(NetResponseSchema.MSG_RESPONSE_101_FIELD_PAYLOAD_3);
									positionMemoData[stateIdx] = state = 5;
									PipeWriter.publishWrites(targetPipe);									
									foundWork = finishAndRelease(true, i, stateIdx, ccId, pipe, cc); 
									
									assert(positionMemoData[(i<<2)+1] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[(i<<2)+1]+" != "+Pipe.releasePendingByteCount(input[i]);							
									break;
								} else {
									return;//we have no data and need more.
								}
							}
							if (3!=state) {
								break;
							}
	
					case 3: //PAYLOAD READING WITH CHUNKS	
						    assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
							long chunkRemaining = payloadLengthData[i];
							DataOutputBlobWriter<NetResponseSchema> writer3 = PipeWriter.outputStream(targetPipe);
							do {
								
								//logger.info("****************************  chunk remainining {} ",chunkRemaining);
								
								if (0==chunkRemaining) {
									
									assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
									int startingLength3 = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);	
										
//									System.out.print("NEW BLOCK TEXT: ");
//									trieReader.debugAsUTF8(trieReader, System.out, 100,false);
//									System.out.println();
									
									int chunkId = (int)TrieParserReader.parseNext(trieReader, chunkMap);
							
									if (chunkId < 0) {
										
										//restore position so we can debug.
										TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
										
										int readingPos = trieReader.sourcePos;
										
//										boolean debug = false;
//										if (debug) {
//											ByteArrayOutputStream ist = new ByteArrayOutputStream();
//											trieReader.debugAsUTF8(trieReader, new PrintStream(ist), 100,false);
//											byte[] data = ist.toByteArray();
//										
//											assert (trieReader.sourceLen==0 || (  (data[0]>='0' && data[0]<='9') || (data[0]>='a' && data[0]<='f')    )) : "http parse, non hex value found at "+readingPos+" data: "+new String(data);
//										}
										
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
									
										System.err.println("quit early but may have work AAA");									
										
										
										assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe)+" pos "+trieReader.sourcePos+"  "+pipe;
										
										return;	//not enough data yet to parse try again later
									}					
									foundWork = true;								
									chunkRemaining = TrieParserReader.capturedLongField(trieReader,0);
									
									//logger.info("*** parsing new HTTP payload of size {}",chunkRemaining);
									
									if (0==chunkRemaining) {
										
										//TODO: Must add parse support for trailing headers!, this is a hack for now.
										int headerId = (int)TrieParserReader.parseNext(trieReader, headerMap);
										TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
											
										int consumed = startingLength3 - trieReader.sourceLen;
										runLength += consumed;
										Pipe.releasePendingAsReadLock(pipe, consumed);
										subBytes(consumed, pipe, i, trieReader.sourceLen);
//										if (pipe.contentRemaining(pipe)>0  &&  !Pipe.hasContentToRead(pipe)) {
//											throw new RuntimeException("no match");	
//										}
										
										
										if (/*trieReader.sourceLen!=0 ||*/ END_OF_HEADER_ID!=headerId /*|| Pipe.contentRemaining(input[i])>0*/) {
											System.err.println("ERROR "+headerId+"  "+pipe);
											TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
											trieReader.debugAsUTF8(trieReader, System.err, 20, false);
											requestShutdown();
										}
										
										//NOTE: input is low level, TireParser is using low level take
										//      writer output is high level;									
										int len = writer3.closeHighLevelField(NetResponseSchema.MSG_RESPONSE_101_FIELD_PAYLOAD_3);
										positionMemoData[stateIdx] = state = 5;
										PipeWriter.publishWrites(targetPipe);	
										foundWork = finishAndRelease(foundWork, i, stateIdx, ccId, pipe, cc); 
										
										
										assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe)+" pos "+trieReader.sourcePos+"  "+pipe;
										
										
										break;
									} else {
										
										int consumed = startingLength3 - trieReader.sourceLen;
										runLength += consumed;
										Pipe.releasePendingAsReadLock(pipe, consumed);
										subBytes(consumed, pipe, i, trieReader.sourceLen);
//										if (pipe.contentRemaining(pipe)>0  &&  !Pipe.hasContentToRead(pipe)) {
//											throw new RuntimeException("no match");	
//										}
										
										assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
										TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
									}
								}				
								
								
								////////
								//normal copy of data for chunk
								////////
								
								int temp3 = TrieParserReader.parseCopy(trieReader, chunkRemaining, writer3);
								if (temp3>=0) {
									foundWork = true;
								}
				//					System.out.println("    chunk removed total of "+temp3);
								chunkRemaining -= temp3;
								
								assert(chunkRemaining>=0);
								
								if (chunkRemaining==0) {
									//NOTE: assert of these 2 bytes would be a good idea right here.
									TrieParserReader.parseSkip(trieReader, 2); //skip \r\n which appears on the end of every chunk
									temp3+=2;
								}
								
								assert(trieReader.sourceLen == Pipe.releasePendingByteCount(pipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(pipe);
								TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
												
								runLength += (temp3);
								Pipe.releasePendingAsReadLock(pipe, temp3);
								subBytes(temp3, pipe, i, trieReader.sourceLen);
								if (pipe.contentRemaining(pipe)>0  &&  !Pipe.hasContentToRead(pipe)) {
									throw new RuntimeException("no match");	
								}
								
							} while (0 == chunkRemaining);
							
							payloadLengthData[i] = chunkRemaining;
							
							assert(positionMemoData[i<<2] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[i<<2]+" != "+Pipe.releasePendingByteCount(input[i]);
							
							break;
					
					case 5: //END SEND ACK
					    foundWork = finishAndRelease(foundWork, i, stateIdx, ccId, pipe, cc);
					    assert(positionMemoData[(i<<2)+1] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[(i<<2)+1]+" != "+Pipe.releasePendingByteCount(input[i]);
						break;
						
				}
				
				
				assert(positionMemoData[(i<<2)+1] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[(i<<2)+1]+" != "+Pipe.releasePendingByteCount(input[i]);
				
			}
			
		} while(foundWork);

	}

	private boolean finishAndRelease(boolean foundWork, int i, final int stateIdx, long ccId,
			Pipe<NetPayloadSchema> pipe, ClientConnection cc) {
		
		assert(5==positionMemoData[stateIdx]);
		
		//only ack when all the data held has been consumed.
		if (trieReader.sourceLen<=0 &&
		    Pipe.contentRemaining(pipe)==0) {	//added second rule to minimize release messages.
			//TODO: if not sent we should stay on ack step.
			foundWork |= sendRelease(stateIdx, ccId, inputPosition, i);
			//may return without setting ack because pipe is full.	
			if (positionMemoData[stateIdx] != 0) {
				logger.info("not finished {})",cc.id);
			}
			
		} else {
			foundWork |= true;						
			positionMemoData[stateIdx] = 0; //next state	
		}
		
		return foundWork;
	}

	private boolean sendRelease(final int stateIdx, long ccId, long[] position, int i) {

		if (Pipe.hasRoomForWrite(ackStop)) {
			int size = Pipe.addMsgIdx(ackStop, ReleaseSchema.MSG_RELEASE_100);
			Pipe.addLongValue(ccId, ackStop);
			Pipe.addLongValue(position[i], ackStop);
			Pipe.confirmLowLevelWrite(ackStop, size);
			Pipe.publishWrites(ackStop);
			positionMemoData[stateIdx] = 0;
						
			position[i] = -1; 
			
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public void shutdown() {
		assert(reportRecordedStates(getClass().getSimpleName()));
	}

}
