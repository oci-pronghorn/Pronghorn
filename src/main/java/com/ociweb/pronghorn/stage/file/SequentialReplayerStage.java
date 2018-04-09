package com.ociweb.pronghorn.stage.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FragmentWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.hash.LongHashSet;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadConsumerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadProducerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadReleaseSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreConsumerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreProducerSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialCtlSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialRespSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SequentialReplayerStage extends PronghornStage {

	private final static Logger logger = LoggerFactory.getLogger(SequentialReplayerStage.class);
	
	private final Pipe<PersistedBlobStoreConsumerSchema> storeConsumerRequests;
	private final Pipe<PersistedBlobStoreProducerSchema> storeProducerRequests;
	
	private final Pipe<PersistedBlobLoadReleaseSchema>  loadReleaseResponses;
    private final Pipe<PersistedBlobLoadConsumerSchema> loadConsumerResponses;
    private final Pipe<PersistedBlobLoadProducerSchema> loadProducerResponses;
    
	private final Pipe<SequentialCtlSchema>[] fileControl;
	private final Pipe<SequentialRespSchema>[] fileResponse;
	private final Pipe<RawDataSchema>[] fileOutput;
	private final Pipe<RawDataSchema>[] fileInput;
	
	private LongHashSet mapSet;
	private int activeFile;
	private int waitCount;
	
	private final static byte MODE_WRITE = 0;
	private final static byte MODE_READ_RELEASES = 1;
	private final static byte MODE_READ_DATA = 2;
	private final static byte MODE_COMPACT_READ_RELEASES = 3;
	private final static byte MODE_COMPACT_READ_DATA = 4;
			
	private int mode = MODE_WRITE;
		
	private int activeIdx = -1;
	private int requestsInFlight = 0;
	
	private int biggestIdx = -1;
	private long biggestSize = 0;
	private int latestIdx = -1;
	private long latestTime = 0;
	
	private byte clearInProgress = 0;
	private boolean isDirty = true;
	private int fileSizeLimit;
	private int fileSizeWritten;
	private long maxId = 0;
	private final NoiseProducer noiseProducer;

	private int shutdownCount = 3;//waits for all 3 files to complete before shutdown
	
	private boolean shutdownInProgress = false;
	
	protected SequentialReplayerStage(GraphManager graphManager, 
			
		            Pipe<PersistedBlobStoreConsumerSchema> storeConsumerRequests, 
		            Pipe<PersistedBlobStoreProducerSchema> storeProducerRequests, 
		            
		            Pipe<PersistedBlobLoadReleaseSchema>  loadReleaseResponses,
		            Pipe<PersistedBlobLoadConsumerSchema> loadConsumerResponses,
		            Pipe<PersistedBlobLoadProducerSchema> loadProducerResponses,
		            
					Pipe<SequentialCtlSchema>[] fileControl,//last file is the ack index file
					Pipe<SequentialRespSchema>[] fileResponse,
					Pipe<RawDataSchema>[] fileWriteData,
					Pipe<RawDataSchema>[] fileReadData,
					
					NoiseProducer noiseProducer	
	            ) {
				
		super(graphManager, join(join(fileResponse, storeConsumerRequests, storeProducerRequests),fileReadData),
				            join(join(fileControl, loadReleaseResponses, loadConsumerResponses, loadProducerResponses),fileWriteData));

		this.storeConsumerRequests = storeConsumerRequests;
		this.storeProducerRequests = storeProducerRequests;
		
		this.loadReleaseResponses = loadReleaseResponses;
		this.loadConsumerResponses = loadConsumerResponses;
		this.loadProducerResponses = loadProducerResponses;
		
		this.noiseProducer = noiseProducer;
		
		this.fileControl = fileControl;
		this.fileResponse = fileResponse;
		this.fileOutput = fileWriteData;
		this.fileInput = fileReadData;
		
		assert(fileControl.length == fileResponse.length);
		assert(fileControl.length == fileReadData.length);
		assert(fileWriteData.length == fileReadData.length);

		this.fileSizeLimit = 10 * Math.max(storeProducerRequests.sizeOfBlobRing,
				            	    loadConsumerResponses.sizeOfBlobRing);
	}

	@Override 
	public void startup() {
		
		assert(fileControl.length == 3);
		int i = fileControl.length-1; //skip the ack index file
		this.waitCount = i;
		assert(waitCount==2) : "only 2 is supported";
		while (--i>=0) {
			Pipe<SequentialCtlSchema> output = fileControl[i];
			Pipe.presumeRoomForWrite(output);
			
			FragmentWriter.write(output, SequentialCtlSchema.MSG_METAREQUEST_3);
			
		}
		
		//will grow as needed
		mapSet = new LongHashSet(15);
		
	}
	
	@Override
	public void shutdown() {
		//logger.info("shutdown");

	}
	
	@Override
	public void run() {

		
		if (shutdownInProgress) {
		
			//only shutdown if we are not in the process of doing some non normal task.
			if (MODE_WRITE == mode && requestsInFlight==0) {
			
				int i = fileControl.length;			
				while (--i >= 0) {
					if (!Pipe.hasRoomForWrite(fileControl[i])) {
						return;//must wait until we have room.
					}
				}
				if (   (!Pipe.hasRoomForWrite(loadConsumerResponses)) 
					|| (!Pipe.hasRoomForWrite(loadProducerResponses))) {
					return;
				}
				
				Pipe.publishEOF(fileControl);
				Pipe.publishEOF(loadConsumerResponses);
				Pipe.publishEOF(loadProducerResponses);				
				
				requestShutdown();
				return;
			}
		}
		
		boolean didWork;
		do {
			didWork = false;
			if (0==waitCount) {
				assert(activeIdx>=0);
				fileResponseProcessing();				
				
				if (MODE_WRITE == mode) { //most common case
					didWork |= writePhase();
				} else if (MODE_READ_DATA == mode) { //second most common case
					didWork |= replayPhase();					
				} else if (MODE_COMPACT_READ_DATA == mode) { //third most common 
					didWork |= compactDataPhase();					
				} else if (MODE_READ_RELEASES == mode) { //only needed when dirty
					didWork |= readReleasedBlockIdsPhase(MODE_READ_DATA);					
				} else if (MODE_COMPACT_READ_RELEASES == mode) {  //only needed when dirty
					didWork |= readReleasedBlockIdsPhase(MODE_COMPACT_READ_DATA);
				}
				
			} else {
				didWork |= initPhase();			
			}
		}while (didWork);
	}
	
	private boolean readReleasedBlockIdsPhase(int nextMode) {

		assert(isDirty) : "only call when dirty";
		boolean didWork = false;
		Pipe<RawDataSchema> input = fileInput[fileInput.length-1];
		
	    while ( mode != nextMode && 
	    		Pipe.hasContentToRead(input)) {
	    	
	    	didWork = true;
		    int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
			        	
		        	DataInputBlobReader<RawDataSchema> fieldByteArray = Pipe.inputStream(input);
		        	int additionalLength = fieldByteArray.accumLowLevelAPIField();

	        		boolean endOfDataDetected = (additionalLength < 0 );
	        		while (   (fieldByteArray.available() >=10) 
	        				|| (endOfDataDetected && (fieldByteArray.available()>0))) {
	        			//only read packed long when we know it will succeed
	        			recordReleaseId(fieldByteArray.readPackedLong());
	        		}
		        	if (endOfDataDetected) {
		        		logger.trace("finished read of all releases");
		        		isDirty = false;
		        		//all the data  has been consumed go to next step.
			        	mode = nextMode;
		        	}
		        	Pipe.confirmLowLevelRead(input, Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1));
				        	
		        break;
		        case -1:
		        	logger.trace("finished read of all releases");
		        	isDirty = false;
		            //all the data  has been consumed go to next step.
		        	mode = nextMode;
		        	Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);				    
		        break;
		    }
		    Pipe.releaseReadLock(input);
		}
		return didWork;
	}
		
	///////////////////////
	///////////////////////
    //The in-memory index of those items released is done with 
	//a simple long set, we only set values and check then
	//when we reload/compact the entire map is cleared.
	private void clearIdMap() {
		LongHashSet.clear(mapSet);
	}
	
	private void recordReleaseId(long releasedId) {
		if (LongHashSet.isFull(mapSet)) {
			mapSet = LongHashSet.doubleClone(mapSet);
		}
		LongHashSet.setItem(mapSet, releasedId);
	}
	
	private boolean isReleased(long fieldBlockId) {
		return LongHashSet.hasItem(mapSet, fieldBlockId);
	}

	//////////////////////


	private boolean initPhase() {
		boolean didWork = false;
		//watch for the files for startup
		int i = fileResponse.length-1; //skips the index file
		while (--i>=0) {
			
			Pipe<SequentialRespSchema> input = fileResponse[i];
			
			while (Pipe.hasContentToRead(input)) {
			    int msgIdx = Pipe.takeMsgIdx(input);
						    
			    didWork = true;
			    switch(msgIdx) {
			    	case SequentialRespSchema.MSG_METARESPONSE_2:
			    		long fieldSize = Pipe.takeLong(input);
			    		long fieldDate = Pipe.takeLong(input);
			    	
			    		//logger.info("meta response for {} field size {}, fielddate {} ",i,fieldSize,fieldDate);
			    		
			    		if (fieldSize>biggestSize) {
			    			biggestSize = fieldSize;
			    			biggestIdx =  i;
			    		}
			    		if (fieldDate>latestTime) {
			    			latestTime = fieldDate;
			    			latestIdx = -1;
			    		}
	
			    		if (--waitCount == 0) {
			    			//logger.info("down to zero");
			    			if (latestIdx > 0) {
			    				activeIdx = latestIdx;
			    			} else {
			    				activeIdx = biggestIdx;
			    			}
			    			//this is only the case if everything is empty 
			    			if (activeIdx<0) {
			    				activeIdx = 0;//just pick zero.
			    			}
			    			assert(activeIdx>=0);
			    		}
			    		assert(waitCount>=0) : "Bad response data detected";
			    	break;
			        default:
			        	throw new RuntimeException("unexpected response message on startup");
			        
			    }
			    Pipe.confirmLowLevelRead(input, Pipe.sizeOf(SequentialRespSchema.instance, msgIdx));
			    Pipe.releaseReadLock(input);
			}			
		}
		return didWork;
	}
	
	private void fileResponseProcessing() {

		
		int i = fileResponse.length;
		while (--i>=0) {
		
			Pipe<SequentialRespSchema> input = fileResponse[i];
			
			while (Pipe.hasContentToRead(input)
					&& Pipe.hasRoomForWrite(loadProducerResponses)
					&& Pipe.hasRoomForWrite(loadReleaseResponses)
					) {
			    int msgIdx = Pipe.takeMsgIdx(input);
			    switch(msgIdx) {
			        case SequentialRespSchema.MSG_CLEARACK_1:
			        	clearInProgress--;
			        	assert(clearInProgress>=0);
					break;
			        case SequentialRespSchema.MSG_METARESPONSE_2:
						//long fieldSize = PipeReader.readLong(input,SequentialFileResponseSchema.MSG_METARESPONSE_2_FIELD_SIZE_11);
						//long fieldDate = PipeReader.readLong(input,SequentialFileResponseSchema.MSG_METARESPONSE_2_FIELD_DATE_11);
						logger.info("got back a meta response but at this point we are not expecting one");
					break;
			        case SequentialRespSchema.MSG_WRITEACK_3:
			        	
			        	requestsInFlight--;
			        	long ackId = Pipe.takeLong(input);
			           	if (0==i || 1==i) {
			           		Pipe.presumeRoomForWrite(loadProducerResponses);			           		
							FragmentWriter.writeL(loadProducerResponses, PersistedBlobLoadProducerSchema.MSG_ACKWRITE_11, ackId);	
			        	} else {
			        		Pipe.presumeRoomForWrite(loadReleaseResponses);
			        		FragmentWriter.writeL(loadReleaseResponses, PersistedBlobLoadReleaseSchema.MSG_ACKRELEASE_10, ackId);
			        	}
			        break;
			        case -1:
			        	//logger.info("got shutdown request for file {} countdown {}",i,shutdownCount);
			            if (--shutdownCount == 0) {
			            	shutdownInProgress = true;
			            }
			        break;
			    }
			    
			    Pipe.confirmLowLevelRead(input, Pipe.sizeOf(SequentialRespSchema.instance, msgIdx));			    			    
			    Pipe.releaseReadLock(input);
			    
			}
		}
	}
	
	private boolean replayPhase() {

		boolean didWork = false;
		Pipe<RawDataSchema> input = fileInput[activeFile];
				
		logger.trace("replay of file {} data {}",activeFile,input);
		
		while ( MODE_READ_DATA == mode &&
				Pipe.hasRoomForWrite(loadConsumerResponses) &&
				Pipe.hasRoomForWrite(loadProducerResponses) &&
				Pipe.hasContentToRead(input)) {
			
		    didWork = true;
			int msgIdx = Pipe.takeMsgIdx(input);
			
			//logger.trace("replay data msgIdx: {}",msgIdx);
			
		    switch(msgIdx) {
		        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
	
		        	DataInputBlobReader<RawDataSchema> reader = Pipe.inputStream(input);
		        	int payloadLen = Pipe.peekInt(input, 1); //length is after meta data
		        	
		        	reader.accumLowLevelAPIField();
		        	
		        	int consumed = 0;
		        	if (payloadLen>=0) {		        		        		
			        	consumed = consumeBlock(reader);			        	
		        	} else {
		        		//logger.info("end of replay");
			        	//when payloadlen == -1 then
		        		//we publish end of replay...			        	
		        		
		        		Pipe.presumeRoomForWrite(loadConsumerResponses);
		        		int size = Pipe.addMsgIdx(loadConsumerResponses, PersistedBlobLoadConsumerSchema.MSG_FINISHREPLAY_9);
		        		Pipe.confirmLowLevelWrite(loadConsumerResponses, size);
		        		Pipe.publishWrites(loadConsumerResponses);
		        	
		        		mode = MODE_WRITE;
		        		requestsInFlight--;
		        	}
		        	Pipe.confirmLowLevelRead(input, Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1));
		        	Pipe.readNextWithoutReleasingReadLock(input);
		        	Pipe.releasePendingAsReadLock(input, consumed);
		        	 
		        	break;
		        case -1:
		        	logger.trace("end of replay and shutdown request");
	        		
		        	Pipe.presumeRoomForWrite(loadConsumerResponses);
	        		int size = Pipe.addMsgIdx(loadConsumerResponses, PersistedBlobLoadConsumerSchema.MSG_FINISHREPLAY_9);
	        		Pipe.confirmLowLevelWrite(loadConsumerResponses, size);
	        		Pipe.publishWrites(loadConsumerResponses);
	        		
	        		mode = MODE_WRITE;
	        		requestsInFlight--;
		        	Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
		        	Pipe.releaseReadLock(input);
	        		break;
		    }
		    
		}
		return didWork;
	
	}

	private int consumeBlock(DataInputBlobReader<RawDataSchema> reader) {
		
    	int mark = reader.position();
    	
    	final long id = reader.readPackedLong();
    	final int totalLength = reader.readInt();
    			
		if (reader.available() < totalLength) {
			//we have not yet accumulated all the data so try again later.
			reader.position(mark);
			return 0;
		}
		
		//without this check we might go off into strange data
		if ((id<0) || (id>maxId)) {
			logger.info(" Reading the active data file, and discovered it is corrupt, send request to clean");
		    throw new UnsupportedOperationException();
		} else { 		
			
			if (!isReleased(id)) {
				//noise data stored to reduce data leakage.
				final int skipLength = reader.readPackedInt();
				reader.absolutePosition(reader.absolutePosition()+skipLength);
				
				final int dataLength = reader.readPackedInt();
				
				//not released so this must be sent back to the caller
			    Pipe.presumeRoomForWrite(loadConsumerResponses);
			    int size = Pipe.addMsgIdx(loadConsumerResponses, PersistedBlobLoadConsumerSchema.MSG_BLOCK_1);
			    Pipe.addLongValue(id, loadConsumerResponses);

			    DataOutputBlobWriter<PersistedBlobLoadConsumerSchema> outStr = Pipe.outputStream(loadConsumerResponses);
			    DataOutputBlobWriter.openField(outStr);
			
			    reader.readInto(outStr, dataLength);
			    					    	    
			    final int structLength = reader.readPackedInt();
			    if (structLength>0) {					    	    	
			    	outStr.structured().fullIndexWriteFrom(structLength, reader);
			    	DataOutputBlobWriter.setStructType(outStr, reader.readPackedInt());
			    }
			    
			    DataOutputBlobWriter.closeLowLevelField(outStr);
			    
			    
			    Pipe.confirmLowLevelWrite(loadConsumerResponses, size);
			    Pipe.publishWrites(loadConsumerResponses);
			    //logger.info("reading block for replay");	
			    
			} else {
				//released so do not repeat back to the caller
				reader.absolutePosition(reader.absolutePosition()+totalLength);
			}
			
		}
		
		return reader.position()-mark;
	}

///////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////

	private boolean shutdownRequestedFromWrite = false;
	
	private boolean writePhase() {
		
		//consume next command
	    boolean didWork = false;	
		while ( Pipe.hasContentToRead(storeProducerRequests) &&
				(0 == clearInProgress) && 
				Pipe.hasRoomForWrite(fileOutput[0]) &&
				Pipe.hasRoomForWrite(fileOutput[1]) &&
				Pipe.hasRoomForWrite(fileOutput[2]) &&
				Pipe.hasRoomForWrite(fileControl[0]) &&
				Pipe.hasRoomForWrite(fileControl[1]) &&
				Pipe.hasRoomForWrite(fileControl[2]) 
				) {

			didWork = true;
		    int msgIdx = Pipe.takeMsgIdx(storeProducerRequests);
		    
		    switch(msgIdx) {
		        case PersistedBlobStoreProducerSchema.MSG_BLOCK_1:
		   
		    		//write this block to the active file.
					final long blockId = Pipe.takeLong(storeProducerRequests);
									
					if (blockId > maxId) {
						maxId = blockId;
					}
					writeBlock(blockId, 
				    		   Pipe.openInputStream(storeProducerRequests), 
				    		   fileOutput[activeIdx], 
				    		   fileControl[activeIdx]);		        	
	
				    detectAndTriggerCompaction();
				break;
		        case -1:
		        	//inform dependent stages that we need to begin the shutdown process
		        	//shutdown can only be done by the reader, the writer must complete all tasks
		        	//until the write ack returns which triggers reader then we can shutdown.
		        	shutdownRequestedFromWrite = true;
		        break;
		    }
		    Pipe.confirmLowLevelRead(storeProducerRequests, Pipe.sizeOf(PersistedBlobStoreProducerSchema.instance, msgIdx));
		    Pipe.releaseReadLock(storeProducerRequests);
		
		}
			
		while ( 
				Pipe.hasContentToRead(storeConsumerRequests) &&
				(0 == clearInProgress) && 
				//for release do not need to wait but replay and clear must wait for in flight to settle down first
				(Pipe.peekMsg(storeConsumerRequests,
						PersistedBlobStoreConsumerSchema.MSG_RELEASE_7) ||
						0==requestsInFlight) &&
				
				Pipe.hasRoomForWrite(loadConsumerResponses) &&
				Pipe.hasRoomForWrite(fileOutput[0]) &&
				Pipe.hasRoomForWrite(fileOutput[1]) &&
				Pipe.hasRoomForWrite(fileOutput[2]) &&
				Pipe.hasRoomForWrite(fileControl[0]) &&
				Pipe.hasRoomForWrite(fileControl[1]) &&
				Pipe.hasRoomForWrite(fileControl[2]) 								
				) {

			didWork = true;
		    int msgIdx = Pipe.takeMsgIdx(storeConsumerRequests);
		    
		    switch(msgIdx) {
		       				
		        case PersistedBlobStoreConsumerSchema.MSG_RELEASE_7:
		    		//write this id to the release data file
		        	storeReleaseOfId(Pipe.takeLong(storeConsumerRequests));
				break;
		        case PersistedBlobStoreConsumerSchema.MSG_REQUESTREPLAY_6:
		        	requestReplayOfStoredBlocks();
				break;
		        case PersistedBlobStoreConsumerSchema.MSG_CLEAR_12:
		        	clearAllStoredData();
				break;
		        case -1:
		        	//inform dependent stages that we need to begin the shutdown process
		        	Pipe.publishEOF(fileControl);
		        	shutdownRequestedFromWrite = false;//clear since we got the shudown here.
		        break;
		    }
		    Pipe.confirmLowLevelRead(storeConsumerRequests, Pipe.sizeOf(PersistedBlobStoreConsumerSchema.instance, msgIdx));
		    Pipe.releaseReadLock(storeConsumerRequests);
		
		}
		
		if (shutdownRequestedFromWrite) {
			if (!Pipe.hasContentToRead(storeConsumerRequests)) {
				logger.warn("Consumer must request shutdown but producer requested the shutdown and consumer stopped consuming",new Exception());
				//inform dependent stages that we need to begin the shutdown process
	        	Pipe.publishEOF(fileControl);
			}
		}
		
		return didWork;
	}

	private void clearAllStoredData() {
		int i = fileControl.length;
		clearInProgress = (byte)i;
		clearIdMap();
		isDirty = false;
		fileSizeWritten = 0;
		while (--i>=0) {
			//clear every file
			Pipe<SequentialCtlSchema> output = fileControl[i];
			Pipe.presumeRoomForWrite(output);
			FragmentWriter.write(output, SequentialCtlSchema.MSG_CLEAR_2);

		}
	}

	private void requestReplayOfStoredBlocks() {

		//////////
		//before replay signal encryption to wrap up
		/////////
		
		int size = Pipe.addMsgIdx(fileOutput[activeIdx], RawDataSchema.MSG_CHUNKEDSTREAM_1);
		Pipe.addNullByteArray(fileOutput[activeIdx]);
		Pipe.confirmLowLevelWrite(fileOutput[activeIdx], size);
		Pipe.publishWrites(fileOutput[activeIdx]);
		
		//TODO: test to ensure that the index is written as well.
		size = Pipe.addMsgIdx(fileOutput[fileOutput.length-1], RawDataSchema.MSG_CHUNKEDSTREAM_1);
		Pipe.addNullByteArray(fileOutput[fileOutput.length-1]);
		Pipe.confirmLowLevelWrite(fileOutput[fileOutput.length-1], size);
		Pipe.publishWrites(fileOutput[fileOutput.length-1]);
		
		
		//////////
	
		requestsInFlight++;//will be cleared at the end of the replay
		if (isDirty) {
			//clear known release so we can reload them from storage.
			clearIdMap();
			//switch to to read releases mode
			mode = MODE_READ_RELEASES;
			Pipe<SequentialCtlSchema> output1 = fileControl[fileControl.length-1];
			//request these two files to be played back to us
			Pipe.presumeRoomForWrite(output1);
			FragmentWriter.write(output1, SequentialCtlSchema.MSG_REPLAY_1);
			
		} else {
			mode = MODE_READ_DATA;
		}
		Pipe<SequentialCtlSchema> output = fileControl[activeIdx];
		//both cases need this data
		Pipe.presumeRoomForWrite(output);
		FragmentWriter.write(output, SequentialCtlSchema.MSG_REPLAY_1);

		//tell consumer to get ready we are about to send data
		Pipe.presumeRoomForWrite(loadConsumerResponses);
		FragmentWriter.write(loadConsumerResponses, PersistedBlobLoadConsumerSchema.MSG_BEGINREPLAY_8);
	}

	private void storeReleaseOfId(long fieldBlockId) {

		 //keep in sync				     
		 recordReleaseId(fieldBlockId);
		 
		 //write this id in case of power drop
		 Pipe<RawDataSchema> out = fileOutput[fileOutput.length-1];
		 int chunkSize = Pipe.addMsgIdx(out, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		 //logger.info("send out release data to be decrypted");
		 DataOutputBlobWriter<RawDataSchema> chunkStr = Pipe.outputStream(out);
		 DataOutputBlobWriter.openField(chunkStr);
		 chunkStr.writePackedLong(fieldBlockId);
		 DataOutputBlobWriter.closeLowLevelField(chunkStr);
		 
		 Pipe.confirmLowLevelWrite(out, chunkSize);
		 Pipe.publishWrites(out);
		 Pipe<SequentialCtlSchema> output = fileControl[fileControl.length-1];
		 
		 Pipe.presumeRoomForWrite(output);
		 FragmentWriter.writeL(output, SequentialCtlSchema.MSG_IDTOSAVE_4, fieldBlockId);
	
		 requestsInFlight++;
		  		 
	}


	private void writeBlock(long blockId, DataInputBlobReader<?> data,
			                Pipe<RawDataSchema> pipe,
			                Pipe<SequentialCtlSchema> control) {
		
		
		//logger.info("write output data for encrypt to pipe "+pipe.id);

		
		int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		DataOutputBlobWriter<RawDataSchema> str = Pipe.outputStream(pipe);
		DataOutputBlobWriter.openField(str);
				
		str.writePackedLong(blockId);/////packed LONG for the ID
			
		int dataLength = data.available();		
			
		assert(dataLength+15<pipe.maxVarLen) : "Outgoing pipe to filesystem is too small";
		
		//NOTE: must write total packed length up front or the messages will get split
		int indexOfLength = str.absolutePosition();
		str.writeInt(0); //length place holder
		int dataBeginPosition=str.position();
		
		
		if (null==noiseProducer) {
			str.writePackedInt(0);	
		} else {
			//Noise must NOT push us past the maxVarLen size so compute how big it can be.
			int realDataSize = 8+dataLength+(3*10); //for length packed ints
			if (data.isStructured()) {
				realDataSize += data.structured().fullIndexSizeInBytes();
			}
			System.err.println("full producer size: "+str.getPipe().maxVarLen);
			//we now have the size so build the noise.			
			//NOTE: also writes leading backed int for size
			noiseProducer.writeNoise(str, blockId, str.getPipe().maxVarLen-realDataSize);			
		}
		
		//logger.trace("wrote length from sequential replayer {} ", length);		
		//str.writePackedInt(totalLength);        /////packed INT for the full data length  
		str.writePackedInt(dataLength); 		
		data.readInto(str, dataLength);        /////then the data 
		
		if (data.isStructured()) {
			int fullIndexSizeInBytes = data.structured().fullIndexSizeInBytes();
			str.writePackedInt(fullIndexSizeInBytes);
			if (fullIndexSizeInBytes>0) {
				data.structured().fullIndexReadInto(str);
				str.writePackedInt(DataInputBlobReader.getStructType(data));
			}			
		} else {
			str.writePackedInt(0);
		}
		
		DataOutputBlobWriter.write32(
				pipe.blobRing, pipe.blobMask,
				indexOfLength, str.position()-dataBeginPosition);
	
		
		DataOutputBlobWriter.closeLowLevelField(str);
		
		fileSizeWritten += str.length();
		
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
		
		Pipe.presumeRoomForWrite(control);
		FragmentWriter.writeL(control, SequentialCtlSchema.MSG_IDTOSAVE_4, blockId);
	
		requestsInFlight++;

	}

	private void detectAndTriggerCompaction() {
		//if the file size is large it is time to roll-over to the next one
		//but if the released count is empty there is no point
		//if the map set is large and mostly full then also compact.
		if ( ((fileSizeWritten > fileSizeLimit) || (LongHashSet.size(mapSet)>(1<<21)))				
				&& (LongHashSet.isPctFull(mapSet,.33))			
	    	) {		        	
        	//ensure new file is clear
        	clearInProgress = 1;
			Pipe<SequentialCtlSchema> output2 = fileControl[1&(activeIdx+1)];
			
			Pipe.presumeRoomForWrite(output2);
			FragmentWriter.write(output2, SequentialCtlSchema.MSG_CLEAR_2);
			
        	fileSizeWritten = 0;
			if (isDirty) {
				//clear known release so we can reload them from storage.
				clearIdMap();
				//switch to to read releases mode
				mode = MODE_COMPACT_READ_RELEASES;
				Pipe<SequentialCtlSchema> output1 = fileControl[fileControl.length-1];
				//request these two files to be played back to us
				Pipe.presumeRoomForWrite(output1);
				FragmentWriter.write(output1, SequentialCtlSchema.MSG_REPLAY_1);
	
			    //when it is done it will change mode to MODE_COMPACT_READ_DATA
			} else {
				mode = MODE_COMPACT_READ_DATA;
			}
			Pipe<SequentialCtlSchema> output = fileControl[activeIdx];
			//both cases need this data
			Pipe.presumeRoomForWrite(output);
			FragmentWriter.write(output, SequentialCtlSchema.MSG_REPLAY_1);
		}
	}

    /////////////
	////////////
	/////////	
	
	private boolean compactDataPhase() {		
		
		boolean didWork = false;
		
		int newActiveIdx = 1&(activeIdx+1);
		
		Pipe<RawDataSchema> input = fileInput[activeFile];
		
		while ( mode != MODE_WRITE &&
				Pipe.hasRoomForWrite(fileOutput[newActiveIdx]) &&
				Pipe.hasContentToRead(input)) {
			
		    didWork = true;
			int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
	
		        	DataInputBlobReader<RawDataSchema> reader = Pipe.inputStream(input);
		        	int payloadLen = Pipe.peekInt(input, 1);
		        	int len = reader.openLowLevelAPIField();
		        	assert(payloadLen==len || (len==0 && payloadLen==-1));
		        	
		        	if (payloadLen>=0) {
			        	final long id = reader.readPackedLong();
			        	final int length = reader.readPackedInt();
			        	
			        	//without this check we might go off into strange data
			        	if ((length!=reader.available()) || (id<0) || (id>maxId)) {
			        		logger.info("ERROR 2, the data files are corrupt, send request to clean");
			        	} else {
			        	
					    	if (!isReleased(id)) {
					    		//not released so keep this and write to the new file
					    					
					    	    //store new block.
					    		writeBlock(id, reader, fileOutput[newActiveIdx], fileControl[newActiveIdx]);
		        		
				        	} else {
				        		//released so do not repeat back to the caller
				        		reader.skipBytes(length);
				        	}
			        	}
		        	} else {
			        	//when payloadlen == -1 then
		        		//we publish end of replay...
						Pipe.presumeRoomForWrite(loadConsumerResponses);
						FragmentWriter.write(loadConsumerResponses, PersistedBlobLoadConsumerSchema.MSG_FINISHREPLAY_9);
						
		        		activeIdx = newActiveIdx;
		        		mode = MODE_WRITE;
		        	}
		        	
		        	Pipe.confirmLowLevelRead(input, Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1));
		        	
		        	break;
		        case -1:		        	
					Pipe.presumeRoomForWrite(loadConsumerResponses);
					FragmentWriter.write(loadConsumerResponses, PersistedBlobLoadConsumerSchema.MSG_FINISHREPLAY_9);

	        		activeIdx = newActiveIdx;
	        		mode = MODE_WRITE;
	        		
		        	Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
		        	        		
	        		break;
		    }
		    Pipe.releaseReadLock(input);
		}
		return didWork;
	}

	
	
}
