package com.ociweb.pronghorn.stage.file;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FragmentWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialCtlSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialRespSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SequentialReplayerStage extends PronghornStage {

	private final static Logger logger = LoggerFactory.getLogger(SequentialReplayerStage.class);
	
	private final Pipe<PersistedBlobStoreSchema> storeRequests;
	private final Pipe<PersistedBlobLoadSchema> loadResponses;
    
	private final Pipe<SequentialCtlSchema>[] fileControl;
	private final Pipe<SequentialRespSchema>[] fileResponse;
	private final Pipe<RawDataSchema>[] fileOutput;
	private final Pipe<RawDataSchema>[] fileInput;
	
	private long[] idMap;
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
	private int idMapSize;
	private final long maxId;
	private int shutdownCount = 3;//waits for all 3 files to complete before shutdown
	
	private boolean shutdownInProgress = false;
	
	//TODO: reduce this to fewer pipes.
	
	protected SequentialReplayerStage(GraphManager graphManager, 
			
		            Pipe<PersistedBlobStoreSchema> storeRequests,  //load request
		            Pipe<PersistedBlobLoadSchema> loadResponses,  //Write ack, load done?
		            
					Pipe<SequentialCtlSchema>[] fileControl,//last file is the ack index file
					Pipe<SequentialRespSchema>[] fileResponse,
					Pipe<RawDataSchema>[] fileWriteData,
					Pipe<RawDataSchema>[] fileReadData,
		            
		            byte fileSizeMultiplier, //cycle data after file is this * storeRequests size
		            byte maxIdValueBits //ID values for each block
		            
	            ) {
				
		super(graphManager, join(join(fileResponse, storeRequests),fileReadData), join(join(fileControl, loadResponses),fileWriteData));

		this.storeRequests = storeRequests;
		this.loadResponses = loadResponses;
		this.fileControl = fileControl;
		this.fileResponse = fileResponse;
		this.fileOutput = fileWriteData;
		this.fileInput = fileReadData;
		
		assert(fileControl.length == fileResponse.length);
		assert(fileControl.length == fileReadData.length);
		assert(fileWriteData.length == fileReadData.length);
		
		this.idMapSize = 1<<(maxIdValueBits-6);
		this.maxId = 1L<<maxIdValueBits;
		this.fileSizeLimit = fileSizeMultiplier*Math.max(storeRequests.sizeOfBlobRing, loadResponses.sizeOfBlobRing);
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
		
		//create space for full filter
		this.idMap = new long[idMapSize];	
		
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
				if (!Pipe.hasRoomForWrite(loadResponses)) {
					return;
				}
				Pipe.publishEOF(fileControl);
				Pipe.publishEOF(loadResponses);
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
	        			long readPackedLong = fieldByteArray.readPackedLong();
						recordReleaseId(readPackedLong);
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

	private void clearIdMap() {
		Arrays.fill(idMap,0);
	}
	
	private void recordReleaseId(long releasedId) {
		idMap[(int)(releasedId>>6)] |= (1<<(0x3F & (int)releasedId));
	}
	
	private boolean isReleased(long fieldBlockId) {
		return  (0 != (idMap[(int)(fieldBlockId>>6)]&(1<<(0x3F & (int)fieldBlockId))));
	}

	//////////////////////
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
			
			while (Pipe.hasContentToRead(input)) {
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
			           		Pipe.presumeRoomForWrite(loadResponses);			           		
							FragmentWriter.writeL(loadResponses, PersistedBlobLoadSchema.MSG_ACKWRITE_11, ackId);	
			        	} else {
			        		Pipe.presumeRoomForWrite(loadResponses);
			        		FragmentWriter.writeL(loadResponses, PersistedBlobLoadSchema.MSG_ACKRELEASE_10, ackId);
			        	}
			        break;
			        case -1:
			        	logger.trace("got shutdown request for file {}",i);
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
				Pipe.hasRoomForWrite(loadResponses) &&
				Pipe.hasContentToRead(input)) {
			
		    didWork = true;
			int msgIdx = Pipe.takeMsgIdx(input);
			
			logger.trace("replay data msgIdx: {}",msgIdx);
			
		    switch(msgIdx) {
		        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
	
		        	DataInputBlobReader<RawDataSchema> reader = Pipe.inputStream(input);
		        	int payloadLen = Pipe.peekInt(input, 1); //length is after meta data
		        	int temp = reader.openLowLevelAPIField();		        	
		        	assert(payloadLen==temp || (payloadLen==-1 && temp==0));
	
		        	if (payloadLen>=0) {
		        			        		
			        			
			        	final long id = reader.readPackedLong();
			        	final int length = reader.readPackedInt();
			        	
			        	//without this check we might go off into strange data
			        	if ((length!=reader.available()) || (id<0) || (id>maxId)) {
			        		logger.info("Reading the active data file, and discovered it is corrupt, send request to clean");
			        	} else {
			        	
					    	if (!isReleased(id)) {
					    		//not released so this must be sent back to the caller
					    	    Pipe.presumeRoomForWrite(loadResponses);
					    	    int size = Pipe.addMsgIdx(loadResponses, PersistedBlobLoadSchema.MSG_BLOCK_1);
					    	    Pipe.addLongValue(id, loadResponses);

					    	    DataOutputBlobWriter<PersistedBlobLoadSchema> outStr = Pipe.outputStream(loadResponses);
					    	    DataOutputBlobWriter.openField(outStr);
							
					    	    reader.readInto(outStr, length);		
					    	    DataOutputBlobWriter.closeLowLevelField(outStr);
					    	    
					    	    
					    	    Pipe.confirmLowLevelWrite(loadResponses, size);
					    	    Pipe.publishWrites(loadResponses);
								//logger.info("reading block for replay");		        		
				        	} else {
				        		//released so do not repeat back to the caller
				        		reader.skipBytes(length);
				        		//logger.info("skipping block for replay");
				        	}
			        	}
		        	} else {
		        		//logger.info("end of replay");
			        	//when payloadlen == -1 then
		        		//we publish end of replay...			        	
		        		
		        		Pipe.presumeRoomForWrite(loadResponses);
		        		int size = Pipe.addMsgIdx(loadResponses, PersistedBlobLoadSchema.MSG_FINISHREPLAY_9);
		        		Pipe.confirmLowLevelWrite(loadResponses, size);
		        		Pipe.publishWrites(loadResponses);
		        	
		        		mode = MODE_WRITE;
		        		requestsInFlight--;
		        	}
		        	Pipe.confirmLowLevelRead(input, Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1));
		        	
		        	break;
		        case -1:
		        	logger.trace("end of replay and shutdown request");
	        		
		        	Pipe.presumeRoomForWrite(loadResponses);
	        		int size = Pipe.addMsgIdx(loadResponses, PersistedBlobLoadSchema.MSG_FINISHREPLAY_9);
	        		Pipe.confirmLowLevelWrite(loadResponses, size);
	        		Pipe.publishWrites(loadResponses);
	        		
	        		mode = MODE_WRITE;
	        		requestsInFlight--;
		        	Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
		        	
	        		break;
		    }
		    
		    Pipe.releaseReadLock(input);
		   // System.err.println("after release lock "+input);
		}
		return didWork;
	
	}

///////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////

	private boolean writePhase() {

		
		//consume next command
	    boolean didWork = false;	
		while ( (0 == clearInProgress) && 
				Pipe.hasRoomForWrite(loadResponses) &&
				Pipe.hasRoomForWrite(fileOutput[0]) &&
				Pipe.hasRoomForWrite(fileOutput[1]) &&
				Pipe.hasRoomForWrite(fileOutput[2]) &&
				Pipe.hasRoomForWrite(fileControl[0]) &&
				Pipe.hasRoomForWrite(fileControl[1]) &&
				Pipe.hasRoomForWrite(fileControl[2]) &&
				
				//write block & release do not need to wait but replay and clear must wait for in flight to settle down first
				(Pipe.peekMsg(storeRequests, PersistedBlobStoreSchema.MSG_BLOCK_1, PersistedBlobStoreSchema.MSG_RELEASE_7) ||
				 0==requestsInFlight) &&
								
				Pipe.hasContentToRead(storeRequests)) {

			
			//TODO: we want to create 2 pipes one for write and one for release.
			//      if we already have the releases then do no do the write....
			
			didWork = true;
		    int msgIdx = Pipe.takeMsgIdx(storeRequests);
		    logger.trace("new store request commend {}",msgIdx);
		    
		    switch(msgIdx) {
		        case PersistedBlobStoreSchema.MSG_BLOCK_1:
		    		//write this block to the active file.
				    writeBlock(Pipe.takeLong(storeRequests), 
				    		   Pipe.openInputStream(storeRequests), 
				    		   fileOutput[activeIdx], 
				    		   fileControl[activeIdx]);		        	
		        	detectAndTriggerCompaction();
				break;
		        case PersistedBlobStoreSchema.MSG_RELEASE_7:
		    		//write this id to the release data file
		        	storeReleaseOfId(Pipe.takeLong(storeRequests));
				break;
		        case PersistedBlobStoreSchema.MSG_REQUESTREPLAY_6:
		        	requestReplayOfStoredBlocks();
				break;
		        case PersistedBlobStoreSchema.MSG_CLEAR_12:
		        	clearAllStoredData();
				break;
		        case -1:
		        	logger.trace("send shutdown to file control");
		        	//inform dependent stages that we need to begin the shutdown process
		        	Pipe.publishEOF(fileControl);
		        break;
		    }
		    Pipe.confirmLowLevelRead(storeRequests, Pipe.sizeOf(PersistedBlobStoreSchema.instance, msgIdx));
		    Pipe.releaseReadLock(storeRequests);
	
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
		Pipe.presumeRoomForWrite(loadResponses);
		FragmentWriter.write(loadResponses, PersistedBlobLoadSchema.MSG_BEGINREPLAY_8);
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
			                Pipe<RawDataSchema> pipe, Pipe<SequentialCtlSchema> control) {
		
		
		//logger.info("write output data for encrypt to pipe "+pipe.id);

		
		int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		DataOutputBlobWriter<RawDataSchema> str = Pipe.outputStream(pipe);
		DataOutputBlobWriter.openField(str);
		
		str.writePackedLong(blockId);/////packed LONG for the ID
		int length = data.available();
		
		assert(length+15<pipe.maxVarLen) : "Outgoing pipe to filesystem is too small";
		
		str.writePackedInt(length);        /////packed INT for the data length     
		data.readInto(str, length);        /////then the data   
		
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
		if (fileSizeWritten > fileSizeLimit) {
		        	
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
						Pipe.presumeRoomForWrite(loadResponses);
						FragmentWriter.write(loadResponses, PersistedBlobLoadSchema.MSG_FINISHREPLAY_9);
						
		        		activeIdx = newActiveIdx;
		        		mode = MODE_WRITE;
		        	}
		        	
		        	Pipe.confirmLowLevelRead(input, Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1));
		        	
		        	break;
		        case -1:
					Pipe.presumeRoomForWrite(loadResponses);
					FragmentWriter.write(loadResponses, PersistedBlobLoadSchema.MSG_FINISHREPLAY_9);

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
