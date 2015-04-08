package com.ociweb.pronghorn.stage.stream;

import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.releaseReadLock;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;

import java.io.IOException;
import java.io.OutputStream;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ToOutputStreamStage extends PronghornStage {

	private final RingBuffer inputRing;
	private final OutputStream outputStream;
	private final int step;
	private final boolean eol;
	
	public ToOutputStreamStage(GraphManager gm, RingBuffer inputRing, OutputStream outputStream, boolean eol) {
		super(gm,inputRing,NONE);
		this.inputRing = inputRing;
		this.outputStream = outputStream;
		this.step =  FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		//this blind byte copy only works for this simple message type, it is not appropriate for other complex types
		if (RingBuffer.from(inputRing) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This method can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		this.eol = eol;
	}

		@Override
		public void run() {
			
			try {				
					
				//NOTE: This can be made faster by looping and summing all the lengths to do one single copy to the output stream
				//      That change may however increase latency.
				
				int byteMask = inputRing.byteMask;
				int byteSize = byteMask+1;
				
				while (RingBuffer.contentToLowLevelRead(inputRing, step)) {
					        		                        	    	                        		           
					
					int msgId = RingBuffer.takeMsgIdx(inputRing);
												
					if (msgId<0) { //exit logic
						new Exception("old style close detected, please fix.").printStackTrace();  ///TODO delete this code
						
					} else {    
						RingBuffer.confirmLowLevelRead(inputRing, step);
				    	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer.
				    	int len = takeRingByteLen(inputRing);
				    	if (len>0) {            	
							byte[] data = byteBackingArray(meta, inputRing);
							int off = bytePosition(meta,inputRing,len)&byteMask;
							int len1 = byteSize-off;
							if (len1>=len) {
								//simple add bytes
								outputStream.write(data, off, len); 
							} else {						
								//rolled over the end of the buffer
								outputStream.write(data, off, len1);
								outputStream.write(data, 0, len-len1);
							}
							if (eol) {
								outputStream.write('\n');
							}
							outputStream.flush();
				    	}
				    	releaseReadLock(inputRing);
					}				    
				}			
				
			} catch (IOException e) {
				throw new RuntimeException(e);			
			} 	
								
		}
		
		@Override
		public void shutdown() {
			try {
				outputStream.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	
}
