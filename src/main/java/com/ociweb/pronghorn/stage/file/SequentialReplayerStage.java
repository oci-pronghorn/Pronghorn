package com.ociweb.pronghorn.stage.file;

import java.util.Arrays;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileControlSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileResponseSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SequentialReplayerStage extends PronghornStage {

	
	private final Pipe<PersistedBlobStoreSchema> storeRequests;
	private final Pipe<PersistedBlobLoadSchema> loadResponses;
    
	private final Pipe<SequentialFileControlSchema>[] fileControl;
	private final Pipe<SequentialFileResponseSchema>[] fileResponse;
	private final Pipe<RawDataSchema>[] fileOutput;
	private final Pipe<RawDataSchema>[] fileInput;
	
	private final byte maxIdValueBits;
	private long[] idMap;
	private int activeFile;
	private int waitCount;
	
	private final static byte MODE_WRITE = 0;
	private final static byte MODE_READ_RELEASES = 1;
	private final static byte MODE_READ_DATA = 2;
		
	private int mode = MODE_WRITE;
		
	private int activeIdx = -1;
	
	private int biggestIdx = -1;
	private long biggestSize = 0;
	private int latestIdx = -1;
	private long latestTime = 0;
	
	
	protected SequentialReplayerStage(GraphManager graphManager, 
		            Pipe<PersistedBlobStoreSchema> storeRequests,  //load request
		            Pipe<PersistedBlobLoadSchema> loadResponses,  //Write ack, load done?
		            
					Pipe<SequentialFileControlSchema>[] fileControl,//last file is the ack index file
					Pipe<SequentialFileResponseSchema>[] fileResponse,
					Pipe<RawDataSchema>[] fileOutput,
					Pipe<RawDataSchema>[] fileInput,
		            
		            byte fileSizeMultiplier, //cycle data after file is this * storeRequests size
		            byte maxIdValueBits //ID values for each block
		            
	            ) {
				
		super(graphManager, join(join(fileResponse, storeRequests),fileInput), join(join(fileControl, loadResponses),fileOutput));

		this.storeRequests = storeRequests;
		this.loadResponses = loadResponses;
		this.fileControl = fileControl;
		this.fileResponse = fileResponse;
		this.fileOutput = fileOutput;
		this.fileInput = fileInput;

		this.maxIdValueBits = maxIdValueBits;
	}

	@Override 
	public void startup() {
		
		assert(fileControl.length == 3);
		int i = fileControl.length-1; //skip the ack index file
		this.waitCount = i;
		while (--i>=0) {
			SequentialFileControlSchema.publishMetaRequest(fileControl[i]);
		}
		
		//create space for full filter
		this.idMap = new long[1<<(maxIdValueBits-6)];	
		
	}
	

	private boolean isReleased(long fieldBlockId) {
		int idMapIdx = (int)(fieldBlockId>>6);
		int idMapMsk = 1<<(0x3F & (int)fieldBlockId);
		
		///
		boolean isReleased = (0 != (idMap[idMapIdx]&idMapMsk));
		return isReleased;
	}
	
	@Override
	public void run() {
		boolean didWork;
		do {
			didWork = false;
			if (0==waitCount) {
				if (MODE_WRITE == mode) {
					didWork |= writePhase();
				} else {
					if (MODE_READ_RELEASES == mode) {
						didWork |= readReleasePhase();
					} else {
						assert(MODE_READ_DATA == mode);
						didWork |= replayPhase();
					}
				}
			} else {
				didWork |= initPhase();			
			}
		}while (didWork);
	}

	private boolean readReleasePhase() {
		boolean didWork = false;
		Pipe<RawDataSchema> input = fileInput[fileInput.length-1];
		
	    while (PipeReader.tryReadFragment(input)) {
		    int msgIdx = PipeReader.getMsgIdx(input);
		    switch(msgIdx) {
		        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
		        	DataInputBlobReader<RawDataSchema> fieldByteArray = PipeReader.inputStream(input, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
		        			        	
		        	int len = fieldByteArray.accumHighLevelAPIField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
		        	
		        	if (len>=0) {
			        	boolean endOfDataDetected = false;
			        	while (   (fieldByteArray.available() >=10) 
			        		   || (endOfDataDetected && (fieldByteArray.available()>0))) {
			        		//only read packed long when we know it will succeed
			        		recordReleaseId(fieldByteArray.readPackedLong());
			        	}		        	
		        	} else {
		        		
		        		//end
		        		
		        	}
		        	
		        	
		        break;
		        case -1:
		        	
		           //requestShutdown();
		        break;
		    }
		    PipeReader.releaseReadLock(input);
		}
		
		
		
		//NOTE: when done move to next phase.
		
		
		return didWork;
	}

	private boolean writePhase() {
		//consume next command
	    boolean didWork = false;	
		while (PipeReader.tryReadFragment(storeRequests)) {
			didWork = true;
		    int msgIdx = PipeReader.getMsgIdx(storeRequests);
		    
		    switch(msgIdx) {
		        case PersistedBlobStoreSchema.MSG_BLOCK_1:
		        	//write this block to the active file.
		            //if write is over the threashild then
		        	//    request all release values
		        	//    request replay
		        	//    consume results and write to new next active
		        	//    increment the active swithch.
		        	PersistedBlobStoreSchema.consumeBlock(storeRequests);
		        break;
		        case PersistedBlobStoreSchema.MSG_RELEASE_7:
		        	//write this id to the release data file
		            PersistedBlobStoreSchema.consumeRelease(storeRequests);
		        break;
		        case PersistedBlobStoreSchema.MSG_REQUESTREPLAY_6:
		        	//publish beginning of replay and request the release data.
		        	//after release data is fully loaded then request file replay.
		        	
		            PersistedBlobStoreSchema.consumeRequestReplay(storeRequests);
		        break;
		        case PersistedBlobStoreSchema.MSG_CLEAR_12:
		            PersistedBlobStoreSchema.consumeClear(storeRequests);
		        break;
		        case -1:
		           //requestShutdown();
		        break;
		    }
		    
		    PipeReader.releaseReadLock(storeRequests);
		}
		return didWork;
	}

	private boolean replayPhase() {
		//keep sending until we reach -1 then switch modes
		
		boolean didWork = false;
		Pipe<RawDataSchema> input = fileInput[activeFile];
				
		while ( PipeWriter.hasRoomForWrite(loadResponses) &&
				PipeReader.tryReadFragment(input)) {
			
		    didWork = true;
			int msgIdx = PipeReader.getMsgIdx(input);
		    switch(msgIdx) {
		        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
		        	
//		        	PipeReader.readBytesLength(pipe, loc)
//		        	
//		        	
//		        	DataInputBlobReader<RawDataSchema> fieldByteArray = PipeReader.inputStream(input, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
//		        
//		        	//if -1 then we publish end of replay...
//		        	
//		        	
//		        	PersistedBlobLoadSchema.instance.publishBlock(output, fieldBlockId, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength)
//		        	
		        	
		        	break;
		        case -1:
		        	PipeWriter.publishEOF(loadResponses);
		            requestShutdown();
		            return false;
		       
		    }
		    PipeReader.releaseReadLock(input);
		}
		return didWork;
	}

	private void clearIdMap() {
		Arrays.fill(idMap,0);
	}

	private void recordReleaseId(long releasedId) {
		int idMapIdx = (int)(releasedId>>6);
		int idMapMsk = 1<<(0x3F & (int)releasedId);
		
		idMap[idMapIdx] |= idMapMsk;
	}
	
	private boolean initPhase() {
		boolean didWork = false;
		//watch for the files for startup
		int i = fileResponse.length-1; //skips the index file
		while (--i>=0) {
			
			Pipe<SequentialFileResponseSchema> input = fileResponse[i];
			while (PipeReader.tryReadFragment(input)) {
			    int msgIdx = PipeReader.getMsgIdx(input);
			    didWork = true;
			    switch(msgIdx) {
			    	case SequentialFileResponseSchema.MSG_METARESPONSE_2:
			    		long fieldSize = PipeReader.readLong(input,SequentialFileResponseSchema.MSG_METARESPONSE_2_FIELD_SIZE_11);
			    		long fieldDate = PipeReader.readLong(input,SequentialFileResponseSchema.MSG_METARESPONSE_2_FIELD_DATE_11);
			    	
			    		if (fieldSize>biggestSize) {
			    			biggestSize = fieldSize;
			    			biggestIdx =  i;
			    		}
			    		if (fieldDate>latestTime) {
			    			latestTime = fieldDate;
			    			latestIdx = -1;
			    		}
			    		if (0==waitCount--) {
			    			if (latestIdx>0) {
			    				activeIdx = latestIdx;
			    			} else {
			    				activeIdx = biggestIdx;
			    			}
			    		}
			    		assert(waitCount>=0) : "Bad response data detected";
			    	break;
			        default:
			        	throw new RuntimeException("unexpected response message on startup");
			        
			    }
			    PipeReader.releaseReadLock(input);
			}
			
		}
		return didWork;
		
	}

}
