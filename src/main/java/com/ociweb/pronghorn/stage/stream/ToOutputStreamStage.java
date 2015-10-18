package com.ociweb.pronghorn.stage.stream;

import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteLen;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteMetaData;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ToOutputStreamStage extends PronghornStage {

	private final Pipe<RawDataSchema> inputRing;
	private final OutputStream outputStream;
	private final int step;
	private final boolean eol;
	
	public ToOutputStreamStage(GraphManager gm, Pipe<RawDataSchema> inputRing, OutputStream outputStream, boolean eol) {
		super(gm,inputRing,NONE);
		this.inputRing = inputRing;
		
		
		this.outputStream = outputStream;
		this.step =  FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		this.eol = eol;
	}


	    @Override
		public void run() {
			try {				
					
				//NOTE: This can be made faster by looping and summing all the lengths to do one single copy to the output stream
				//      That change may however increase latency.
				
				int byteMask = inputRing.byteMask;
				int byteSize = byteMask+1;								
				
				while (Pipe.hasContentToRead(inputRing)) {
						
					int msgId = Pipe.takeMsgIdx(inputRing);
					if (msgId<0) {
					    Pipe.releaseReads(inputRing);
					    Pipe.confirmLowLevelRead(inputRing, Pipe.EOF_SIZE);
					    requestShutdown();
					    return;
					}
  
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
			    	} else if (len<0) {
			    	    Pipe.releaseReads(inputRing);
	                    Pipe.confirmLowLevelRead(inputRing, step);
			    	    requestShutdown();
			    	    return;
			    	}
			    	Pipe.releaseReads(inputRing);
			    	Pipe.confirmLowLevelRead(inputRing, step);
 
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
