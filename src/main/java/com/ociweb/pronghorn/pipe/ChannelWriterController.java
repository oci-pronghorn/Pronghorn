package com.ociweb.pronghorn.pipe;

public class ChannelWriterController {

	protected final Pipe<RawDataSchema> pipe;
			
	public ChannelWriterController(Pipe<RawDataSchema> pipe) {
		this.pipe = pipe;
	}
	
	/**
	 * check if connection data can take another message
	 * @return true if there is room for another write
	 */
	public boolean hasRoomForWrite() {
		return Pipe.hasRoomForWrite(pipe);
	}
	
	/**
	 * Open the message for writing
	 * @return returns the ChannelWriter or null if there is no room to write.
	 */
	public ChannelWriter beginWrite() {
		if (Pipe.hasRoomForWrite(pipe)) {
			Pipe.markHead(pipe);
			Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
			return Pipe.openOutputStream(pipe);
		}
		return null;
	}
	
	/**
	 * Dispose of everything written and restore to the way it was before
	 * beginWrite() was called.
	 */
	public void abandonWrite() {
		DataOutputBlobWriter.closeLowLevelField(Pipe.outputStream(pipe));
		Pipe.resetHead(pipe);		
	}
	
	/**
	 * Store the message and move the pointers forward so the data can be
	 * consumed later.
	 */
	public void commitWrite() {
		DataOutputBlobWriter.closeLowLevelField(Pipe.outputStream(pipe));		
		
		Pipe.confirmLowLevelWrite(pipe, Pipe.sizeOf(RawDataSchema.instance,RawDataSchema.MSG_CHUNKEDSTREAM_1));
		Pipe.publishWrites(pipe);
	}

}
