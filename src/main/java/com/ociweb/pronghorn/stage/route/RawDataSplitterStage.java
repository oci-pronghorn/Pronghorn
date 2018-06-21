package com.ociweb.pronghorn.stage.route;

import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.RawDataSchemaUtil;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RawDataSplitterStage extends PronghornStage {

	private final Pipe<RawDataSchema> source; 
    private final Pipe<RawDataSchema>[] targets;
    private int targetPipeIdx = -1;
    private long targetRemaining;
	private DataInputBlobReader<RawDataSchema> inputStream;
    private boolean isShuttingDown = false;
    private boolean isEndOfFile = false;
    
   //TODO: needs unit test
    
    public static RawDataSplitterStage newInstance(GraphManager gm, 
            	Pipe<RawDataSchema> source, 
            	Pipe<RawDataSchema> ... targets) {
    	return new RawDataSplitterStage(gm, source, targets);
    }
    
    /**
     * Read raw data and route blocks to the right target based on data found.
     * On the stream there is a packed int for which pipe gets the data and 
     * second packed long for how much data is sent to that location.  All outputs
     * are sent the end of file marker when the end of the file is reached.
     * It is assumed that each pipe will be consuming multiple chunks of data.
     * 
     * @param source RawDataSchema Pipe to read from
     * @param targets an array of outgoing target pipes
     */
	public RawDataSplitterStage(GraphManager gm, 
			               Pipe<RawDataSchema> source, 
			               Pipe<RawDataSchema> ... targets) {
		super(gm, source,  targets);
		this.source = source;
		this.targets = targets;
		this.inputStream = Pipe.inputStream(source);
		
		//TODO: class needs unit test
		//TODO: class needs to be moved into Pronghorn..
		
	}

	@Override
	public void run() {
		
		processData();//even called when shutting down so we can clear the pipes.
		
		if (!isShuttingDown) {
			if (!isEndOfFile) {
				while (Pipe.hasContentToRead(source)) {
					boolean isEnd = RawDataSchemaUtil.accumulateInputStream(source);
					if (isEnd) {
						if (Pipe.isEndOfPipe(source, Pipe.getWorkingTailPosition(source))) {
							isShuttingDown = true;
						} else { 
							isEndOfFile = true;							
						}
					}
					processData();
				}
			} else {
				if (Pipe.inputStream(source).available()>0) {
					//can only send end of file after all the data has been consumed.
					return;
				}
				//can only send end of file if all the targets have room.
				int i = targets.length;
				while (--i>=0) {
					if (!Pipe.hasRoomForWrite(targets[i])) {
						return;//try again later
					}
				}
				i = targets.length;
				while (--i>=0) {
					Pipe<RawDataSchema> pipe = targets[i];
					int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
					Pipe.addNullByteArray(pipe);
					Pipe.confirmLowLevelWrite(pipe, size);
					Pipe.publishWrites(pipe);
				}
				isEndOfFile = false;
			}
		} else {
			//shutdown process
			int i = targets.length;
			while (--i>=0) {
				if (!Pipe.hasRoomForWrite(targets[i])) {
					return;//try again later
				}
			}
			Pipe.publishEOF(targets);
			requestShutdown();
		}
	}


	private void readHeaderForNextBlock(DataInputBlobReader<RawDataSchema> inputStream) {
		if (inputStream.available( ) >= (ChannelReader.PACKED_INT_SIZE + ChannelReader.PACKED_LONG_SIZE) 
			|| isEndOfFile 
			|| isShuttingDown ) {
			
			int orig = inputStream.available();
			
			targetPipeIdx = inputStream.readPackedInt();
			targetRemaining = inputStream.readPackedLong();
			
			RawDataSchemaUtil.releaseConsumed(source, inputStream, orig);
		}
	}
	
	private void processData() {
		DataInputBlobReader<RawDataSchema> inputStream = Pipe.inputStream(source);
		
		if (targetPipeIdx<0) {
			readHeaderForNextBlock(inputStream);
		}
			
		/////////////////
		//move the data
		/////////////////
		while (targetRemaining>0 && inputStream.available()>0 && Pipe.hasRoomForWrite(targets[targetPipeIdx]) ) {
			
			Pipe<RawDataSchema> t = targets[targetPipeIdx];
			int toCopyLength = (int)Math.min(Math.min(t.maxVarLen, targetRemaining), inputStream.available());
			
			int size = Pipe.addMsgIdx(t, RawDataSchema.MSG_CHUNKEDSTREAM_1);
			DataOutputBlobWriter<RawDataSchema> outputStream = Pipe.openOutputStream(t);
			inputStream.readInto(outputStream, toCopyLength);
			DataOutputBlobWriter.closeLowLevelField(outputStream);
			Pipe.confirmLowLevelWrite(t, size);
			Pipe.publishWrites(t);
			
			Pipe.releasePendingAsReadLock(source, toCopyLength);
			
			targetRemaining -= toCopyLength;
			
			if (targetRemaining<=0) {
				targetPipeIdx = -1;
				
				readHeaderForNextBlock(inputStream); 
				
			}
			
		}
	}


}
