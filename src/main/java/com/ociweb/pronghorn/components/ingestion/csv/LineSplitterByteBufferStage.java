package com.ociweb.pronghorn.components.ingestion.csv;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class LineSplitterByteBufferStage extends PronghornStage {

	protected final ByteBuffer activeByteBuffer;
	
	protected final Pipe outputRing;
	
	protected int quoteCount = 0;
	protected int prevB = -1;
	protected int recordStart = 0;
	
	protected long recordCount = 0;
    private final int stepSize;
    
    public final byte[] quoter;
    protected int shutdownPosition = -1;
    private final Logger log = LoggerFactory.getLogger(LineSplitterByteBufferStage.class);
    
    public LineSplitterByteBufferStage(GraphManager graphManager, ByteBuffer sourceByteBuffer, Pipe outputRing) {
    	super(graphManager, NONE, outputRing);
    	this.activeByteBuffer=sourceByteBuffer;
    	
    	this.outputRing=outputRing;
        
		if (Pipe.from(outputRing) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		
		stepSize = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		
	    //NOTE: this block has constants that could be moved up and out
	    quoter = new byte[256]; //these are all zeros
	    quoter['"'] = 1; //except for the value of quote.						

		if (outputRing.maxAvgVarLen<1) {
			throw new UnsupportedOperationException();
		}
		resetForNextByteBuffer(this);

    }
    
 
    protected static void resetForNextByteBuffer(LineSplitterByteBufferStage lss) {
    	
    	lss.quoteCount = 0;
    	lss.prevB = -1;
    	lss.recordStart = 0;    	
    }
    

    @Override
    public void startup() {
    }
    
	@Override
	public void run() {

		    	shutdownPosition = parseSingleByteBuffer(this, activeByteBuffer);			    	
		    	if (shutdownPosition>=activeByteBuffer.limit()) {
		    		resetForNextByteBuffer(this);
		    		Pipe.publishAllBatchedWrites(outputRing);
		    		requestShutdown();
		    	}
			
	}
	
	public void blockingRun() {
		do {
			shutdownPosition = parseSingleByteBuffer(this, activeByteBuffer);			    	
		} while (shutdownPosition<activeByteBuffer.limit());
    	resetForNextByteBuffer(this);	
    	Pipe.publishAllBatchedWrites(outputRing);
    	requestShutdown();
	}

	int x = 12;
	protected static int parseSingleByteBuffer(LineSplitterByteBufferStage stage, ByteBuffer sourceByteBuffer) {
		 int position = sourceByteBuffer.position();
		 int limit = sourceByteBuffer.limit();
		 
		 
		 if (!Pipe.hasRoomForWrite(stage.outputRing, stage.stepSize)) {
			 return position;
		 }
		 	    
		 if (position<limit) {					    
			 
		    		int b = sourceByteBuffer.get(position);		    							    		

					if (isEOL(b) ) {
												
						//double checks that this is a real EOL an not an embeded char someplace.
						if ('\\' != stage.prevB && (stage.quoteCount&1)==0) {
							int len = position-stage.recordStart;
							assert(len>=0) : "length: "+len;
							//When we do smaller more frequent copies the performance drops dramatically. 5X slower.
							//The copy is an intrinsic and short copies are not as efficient
							
							sourceByteBuffer.position(stage.recordStart);
							Pipe outputRing = stage.outputRing;
							Pipe.confirmLowLevelWrite(stage.outputRing, stage.stepSize);
							
							
							
							Pipe.addMsgIdx(outputRing, 0);
							long temp = Pipe.workingHeadPosition(outputRing);
							int bytePos = Pipe.bytesWorkingHeadPosition(outputRing);    	

							//debug show the lines
							boolean debug = false;
							if (debug) {
								byte[] dst = new byte[len];
								//stage.log.info("len:"+len+" from "+stage.recordStart+" "+sourceByteBuffer.position()+" to "+sourceByteBuffer.limit());
								sourceByteBuffer.get(dst, 0, len);
								sourceByteBuffer.position(stage.recordStart);
								String expected = new String(dst);
								stage.log.info("split:"+expected+"  bp:"+bytePos+" len:"+len+" at "+temp );
								
							}
							
							Pipe.copyByteBuffer(sourceByteBuffer, len, outputRing);
							Pipe.addBytePosAndLen(outputRing, bytePos, len);
														
							stage.recordCount++;

							Pipe.publishWrites(outputRing);

							
							stage.recordStart = position+1;
						}
					} else {
						//may be normal text an may be a quote
						stage.quoteCount += stage.quoter[0xFF&b];
					}

				stage.prevB = b;
		    	position ++;						 
			 
		 }
		 sourceByteBuffer.position(position);
		 return position;
	}


	//LF  10      00001010
	//CR  13      00001101
	private static boolean isEOL(int b) {
		return 10==b || 13==b;
	}


	public long getRecordCount() {
		return recordCount;
	}

}
