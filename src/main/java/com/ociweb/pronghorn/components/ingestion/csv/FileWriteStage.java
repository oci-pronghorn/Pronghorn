package com.ociweb.pronghorn.components.ingestion.csv;

import static com.ociweb.pronghorn.pipe.Pipe.blobMask;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteLen;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteMetaData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FileWriteStage extends PronghornStage {

	private final Pipe inputRing;
	private final FileChannel channel;

	private final int msgSize = RawDataSchema.FROM.fragDataSize[0];
	
	public FileWriteStage(GraphManager gm, Pipe input, FileChannel channel) {
		super(gm,input,NONE);
		this.inputRing = input;
		this.channel = channel;
		assert(Pipe.from(input) == RawDataSchema.FROM);

	}

	@Override
	public void shutdown() {
		try {
			assert(Pipe.contentRemaining(inputRing)<=0) : "still has content to write";
			channel.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void run() {
	    while (Pipe.hasContentToRead(inputRing)) {
	            	        	
	            int msgId = Pipe.takeMsgIdx(inputRing);
	            if (msgId<0) {  	
	                //Pipe.releaseReads(inputRing);
	                //Pipe.confirmLowLevelRead(inputRing, msgSize);
                    Pipe.dump(inputRing);
	                Pipe.releaseAllBatchedReads(inputRing);
	            	assert(Pipe.contentRemaining(inputRing)==0) : "still has content to write";
	            	requestShutdown();
	            	return;
	            }
	            
	        	int meta = takeRingByteMetaData(inputRing);
	        	int len = takeRingByteLen(inputRing);
	        	if (len<=0) {
	        	    //TODO: use logger!
	        	    System.err.println("Warning we have write length of "+len+" "+inputRing);
	        	}

	        	//converting this to the position will cause the byte posistion to increment.
	        	int pos = bytePosition(meta, inputRing, len);//has side effect of moving the byte pointer!!
	        	int mask = blobMask(inputRing);
	        	
	        	ByteBuffer inputByteBuffer= Pipe.wrappedBlobRingA(inputRing); //TODO: A, should this take into account constants?
	        	
	        	int idx = (pos&mask);
                int len1 = (mask+1)-idx;
                if (len1>=len) {
                	//no roll over
                	
                	inputByteBuffer.clear();
                	inputByteBuffer.limit(idx+len);
                	inputByteBuffer.position(idx);
                	try {
						if (len != channel.write(inputByteBuffer)) {
						    throw new RuntimeException("Did not write expected length.");
						}
					} catch (IOException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
                	
                } else {
                	//roll over
                	
                	inputByteBuffer.clear();
                	inputByteBuffer.position(idx);
                	try {
						channel.write(inputByteBuffer);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
                	
                	inputByteBuffer.position(0);
                	inputByteBuffer.limit(len-len1);
                	try {
						channel.write(inputByteBuffer);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}                	
                }
                                
				Pipe.releaseReads(inputRing);  
				Pipe.confirmLowLevelRead(inputRing, msgSize);
                                
				assert(Pipe.contentRemaining(inputRing)>=0) : "still has "+Pipe.contentRemaining(inputRing)+" content to write "+inputRing;
				
		}
	}
	
	
}
