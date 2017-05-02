package com.ociweb.pronghorn.stage.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class PersistedBlobStage extends PronghornStage {

	private static final Logger logger = LoggerFactory.getLogger(PersistedBlobStage.class);
	
	private final Pipe<PersistedBlobStoreSchema> input;
	private final Pipe<PersistedBlobLoadSchema> output;
	private final int fileSizeLimit;
	private final File rootFolder;
	private final int maxIdValueBits;
	private final long maxIdLimit;
	private final long maxIdLimitMask;
	
	private RandomAccessFile[] blobDataFile;
	private RandomAccessFile releasedFile;
	private long[] idMap;
	int activeFile;
	
	public static PersistedBlobStage newInstance(GraphManager graphManager, 
            Pipe<PersistedBlobStoreSchema> storeRequests,  //load request
            Pipe<PersistedBlobLoadSchema> loadResponses,  //Write ack, load done?
            byte fileSizeMultiplier, 
            byte maxIdValueBits,
            File rootFolder) {
		return new PersistedBlobStage(graphManager,storeRequests,loadResponses,fileSizeMultiplier,maxIdValueBits,rootFolder);
	}
	
	public PersistedBlobStage(GraphManager graphManager, 
			                     Pipe<PersistedBlobStoreSchema> storeRequests,  //load request
			                     Pipe<PersistedBlobLoadSchema> loadResponses,  //Write ack, load done?
			                     byte fileSizeMultiplier, 
			                     byte maxIdValueBits,
			                     File rootFolder) {
	
		super(graphManager, storeRequests, loadResponses);
		this.input = storeRequests;
		this.output = loadResponses;
		this.fileSizeLimit = fileSizeMultiplier*Math.max(storeRequests.sizeOfBlobRing, loadResponses.sizeOfBlobRing);
		this.maxIdValueBits = Math.max(6, maxIdValueBits); //never smaller than 6
		this.maxIdLimit = 1<<maxIdValueBits;
		this.maxIdLimitMask = maxIdLimit-1;
		
		if (!rootFolder.isDirectory()) {
			throw new UnsupportedOperationException("Requires directory but found "+rootFolder);
		}
		this.rootFolder = rootFolder;
	}

	
	@Override
	public void startup() {
		//create space for full filter
		this.idMap = new long[1<<(maxIdValueBits-6)];	
				
		//////////////////
		//select which is the active file
		////////////////
		File fileA = new File(rootFolder, "blobDataA.dat");		
		File fileB = new File(rootFolder, "blobDataB.dat");
		if (fileA.exists()) {
			if (fileA.exists()) {
				logger.warn("two storage files found, only expected one. using most recently modified of the two.");		
				if ((fileA.lastModified()>fileB.lastModified())
					&& ((fileA.length()>0) || (fileB.length()==0))		
					) {
					activeFile = 0;
					fileB.delete();
				} else {
					activeFile = 1;
					fileA.delete();
				}
			} else {			
				activeFile = 0;
			}
		} else {
			activeFile = 1;//b may or may not exist do not care
		}
		

		
		////////////////
		//create Access to files
		////////////////
		try {
			blobDataFile = new RandomAccessFile[]{
					new RandomAccessFile(fileA,"rws"),
					new RandomAccessFile(fileB,"rws")		
			};
			releasedFile = new RandomAccessFile(new File(rootFolder, "released.dat"),"rws");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		/////////////
		/////////////
		try {			
			blobDataFile[activeFile].seek(blobDataFile[activeFile].length());
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}	
	}
	
	@Override
	public void run() {
		
		boolean didWork = false;
		
		//do a single operation and return since disk operations are very slow and the caller
		//may want the thread back to continue processing.
		if ( PipeWriter.hasRoomForWrite(output) &&//only consume when output has room.
				PipeReader.tryReadFragment(input)) {
		    int msgIdx = PipeReader.getMsgIdx(input);
		    didWork = true;
		    switch(msgIdx) {
		        case PersistedBlobStoreSchema.MSG_BLOCK_1:
		        	writeBlock(blobDataFile[activeFile], input, output);
		        break;
		        case PersistedBlobStoreSchema.MSG_RELEASE_7:
		        	releaseBlock(releasedFile, input, output);
		        break;
		        case PersistedBlobStoreSchema.MSG_REQUESTREPLAY_6:
		        	replay(blobDataFile[activeFile], output);
		        break;
		        case PersistedBlobStoreSchema.MSG_CLEAR_12:
		        	clear(releasedFile, blobDataFile[activeFile]);
		        break;
		        case -1:
		           requestShutdown();
		        break;
		    }
		    PipeReader.releaseReadLock(input);
		}		
		
		//never cycle files if a caller is waiting for a task to be completed.
		if (!didWork && isTimeToCycleDataFiles(blobDataFile[activeFile], fileSizeLimit)) {
			cycleDataFiles(releasedFile, blobDataFile);
		}
		
	}

	private void cycleDataFiles(RandomAccessFile releasedFile, RandomAccessFile[] blobDataFiles) {
		
		try {
			buildReleaseMap(releasedFile);
		} catch (IOException e) {
			logger.info("unable to cycle the data files, still using old file",e);
			return;
		}
		
		RandomAccessFile blobDataFile = blobDataFiles[activeFile];
		int nextActiveFile = 1&(1+activeFile);
		RandomAccessFile nextBlobDataFile = blobDataFiles[nextActiveFile];
		
		long endOfFile = -1;
		try{
			endOfFile = blobDataFile.length();
			blobDataFile.seek(0);
			nextBlobDataFile.setLength(0);
			
			do {
				long fieldBlockId = blobDataFile.readLong();
				int blockLen = blobDataFile.readInt();
				
				if (!isReleased(fieldBlockId)) {
					
					nextBlobDataFile.writeLong(fieldBlockId);
					nextBlobDataFile.writeInt(blockLen);					
					
					FileChannel thisChannel = blobDataFile.getChannel();
					FileChannel nextChannel = nextBlobDataFile.getChannel();
					
					thisChannel.transferTo(thisChannel.position(), blockLen, nextChannel);
					
				} else {
					blobDataFile.skipBytes(blockLen);
				}
			    				
			} while (blobDataFile.getFilePointer() < endOfFile);
			
			
		} catch (IOException e) {
			logger.info("unable to replay",e);			
		} finally {
			if (endOfFile>=0) {
				try {
					blobDataFile.setLength(0);
					releasedFile.setLength(0); //start over we have filtered out the old ones
					
				} catch (IOException e) {
					logger.info("unable to seek back to end of file ",e);
				}
			}
		}
		
		activeFile = nextActiveFile;
		
	}

	private void buildReleaseMap(RandomAccessFile releasedFile) throws IOException {
		final long fileLength = releasedFile.length();
		releasedFile.seek(0);
		Arrays.fill(idMap,0);
		
		while (releasedFile.getFilePointer() < fileLength) {
			long releasedId = releasedFile.readLong();
			assert(releasedId<maxIdLimit) : "Should not be this large, should be filtered before writing.";
			
			int idMapIdx = (int)(releasedId>>6);
			int idMapMsk = 1<<(0x3F & (int)releasedId);
			
			idMap[idMapIdx] |= idMapMsk; 

		}
	}

	private boolean isTimeToCycleDataFiles(RandomAccessFile blobDataFile, int fileSizeLimit) {
		try {
			return blobDataFile.length() > fileSizeLimit;
		} catch (IOException e) {
			logger.info("unable to check data file length",e);
			return false;
		}
	}

	private void ackWrite(Pipe<PersistedBlobLoadSchema> output, long fieldBlockId) {
	
		//loop will not happen because we already checked for room in run
		while (!PipeWriter.tryWriteFragment(output, PersistedBlobLoadSchema.MSG_ACKWRITE_11)) {
			Thread.yield();
		}			
	    PipeWriter.writeLong(output,
	    		             PersistedBlobLoadSchema.MSG_ACKWRITE_11_FIELD_BLOCKID_3,
	    		             fieldBlockId);	    
	    PipeWriter.publishWrites(output);
		 
	}

	private void clear(RandomAccessFile releaseFile, RandomAccessFile blobDataFile) {
		try {
			releaseFile.setLength(0);
			blobDataFile.setLength(0);
			
		} catch (IOException e) {
			logger.info("unable to clear stored data",e);
		}
	}

	//warning this call will block until complete
	private void replay(RandomAccessFile blobDataFile, Pipe<PersistedBlobLoadSchema> output) {
		
		try {
			buildReleaseMap(releasedFile);
		} catch (IOException e) {
			logger.info("unable to replay the data files",e);
			return;
		}
		
		
		long endOfFile = -1;
		try{
			endOfFile = blobDataFile.length();
			blobDataFile.seek(0);
			
			while (!PipeWriter.tryWriteFragment(output, PersistedBlobLoadSchema.MSG_BEGINREPLAY_8)) {
				Thread.yield();
			}
			PipeWriter.publishWrites(output);
						
			while (blobDataFile.getFilePointer() < endOfFile) {
				long fieldBlockId = blobDataFile.readLong();
				int blockLen = blobDataFile.readInt();
				
				if (!isReleased(fieldBlockId)) {
					while (!PipeWriter.tryWriteFragment(output, PersistedBlobLoadSchema.MSG_BLOCK_1)) {
						Thread.yield();//must back off until pipe messages get consumed.
					}					
				    PipeWriter.writeLong(output,PersistedBlobLoadSchema.MSG_BLOCK_1_FIELD_BLOCKID_3, fieldBlockId);
				    PipeWriter.writeFieldFromDataInput(output, PersistedBlobLoadSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2,
				    									blobDataFile, blockLen);
				    PipeWriter.publishWrites(output);
				} else {
					blobDataFile.skipBytes(blockLen);
				}			    			
			}
			
			while (!PipeWriter.tryWriteFragment(output, PersistedBlobLoadSchema.MSG_FINISHREPLAY_9)) {
			    Thread.yield();
			}
			PipeWriter.publishWrites(output);
						
		} catch (IOException e) {
			logger.info("unable to replay",e);			
		} finally {
			if (endOfFile>=0) {
				try {
					blobDataFile.seek(endOfFile);
				} catch (IOException e) {
					logger.info("unable to seek back to end of file ",e);
				}
			}
		}
		
	}

	private boolean isReleased(long fieldBlockId) {
		int idMapIdx = (int)(fieldBlockId>>6);
		int idMapMsk = 1<<(0x3F & (int)fieldBlockId);
		
		///
		boolean isReleased = (0 != (idMap[idMapIdx]&idMapMsk));
		return isReleased;
	}

	private boolean releaseBlock(RandomAccessFile releasedFile, Pipe<PersistedBlobStoreSchema> input, Pipe<PersistedBlobLoadSchema> output) {
		
		//write release out.
		long blockId = PipeReader.readLong(input, PersistedBlobStoreSchema.MSG_RELEASE_7_FIELD_BLOCKID_3);
				
		if (blockId>=maxIdLimit || blockId<0) {
			//All Id values must be positive and less than the limit this instance was constructed for.
			logger.trace("out of range blockId, limit was {} but the value used was {} so it was trimmed",maxIdLimit,blockId);
			blockId = maxIdLimitMask & blockId;
		}
		
		try {
			releasedFile.writeLong(blockId);
			//loop will not happen since it was checked earlier
			while (!PipeWriter.tryWriteFragment(output, PersistedBlobLoadSchema.MSG_ACKRELEASE_10)) {
				Thread.yield();
			}
			
			PipeWriter.writeLong(output, PersistedBlobLoadSchema.MSG_ACKRELEASE_10_FIELD_BLOCKID_3, 
					blockId);
			
			PipeWriter.publishWrites(output);
			return true;
		} catch (IOException e) {
			logger.info("unable to write release id", e);
			return false;
		} 
	}

	private boolean writeBlock(RandomAccessFile blobDataFile, 
			                   Pipe<PersistedBlobStoreSchema> input, 
			                   Pipe<PersistedBlobLoadSchema> output) {
				
		try {
			
			long fieldBlockId = PipeReader.readLong(input,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BLOCKID_3);
					
			
			if (fieldBlockId>=maxIdLimit || fieldBlockId<0) {
				//All Id values must be positive and less than the limit this instance was constructed for.
				logger.trace("out of range blockId, limit was {} but the value used was {} so it was trimmed",maxIdLimit,fieldBlockId);
				fieldBlockId = maxIdLimitMask & fieldBlockId;
			}
			
			blobDataFile.writeLong(fieldBlockId);
			int readBytesLength = PipeReader.readBytesLength(input,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2);
			blobDataFile.writeInt(readBytesLength);
			PipeReader.readFieldIntoDataOutput(PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2, input, blobDataFile);
			
			ackWrite(output, fieldBlockId);
			
			return true;
		} catch (IOException ioex) {
			//unable to write so now what.
			logger.info("Unable to write block",ioex);
			return false;
		}
		
	}

}
