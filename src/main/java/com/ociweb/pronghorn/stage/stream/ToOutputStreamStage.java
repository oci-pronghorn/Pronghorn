package com.ociweb.pronghorn.stage.stream;

import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteLen;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteMetaData;

import java.io.IOException;
import java.io.OutputStream;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ToOutputStreamStage extends PronghornStage {

	private final Pipe inputRing;
	private final OutputStream outputStream;
	private final int step;
	private final boolean eol;
	
	public ToOutputStreamStage(GraphManager gm, Pipe inputRing, OutputStream outputStream, boolean eol) {
		super(gm,inputRing,NONE);
		this.inputRing = inputRing;
		
		
		this.outputStream = outputStream;
		this.step =  FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		//this blind byte copy only works for this simple message type, it is not appropriate for other complex types
		if (Pipe.from(inputRing) != FieldReferenceOffsetManager.RAW_BYTES) {
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
				
				while (Pipe.contentToLowLevelRead(inputRing, step)) {
						
					int msgId = Pipe.takeMsgIdx(inputRing);
  
					Pipe.confirmLowLevelRead(inputRing, step);
			    	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer.
	    					    			
			    	int len = takeRingByteLen(inputRing);
			    	int off = bytePosition(meta,inputRing,len)&byteMask; 			
			    	
			    	if (len>0) {            	
						byte[] data = byteBackingArray(meta, inputRing);
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
			    	Pipe.releaseReads(inputRing);
			    
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
