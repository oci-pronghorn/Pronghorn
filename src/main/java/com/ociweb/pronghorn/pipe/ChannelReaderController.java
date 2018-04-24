package com.ociweb.pronghorn.pipe;

public class ChannelReaderController {

	protected final Pipe<RawDataSchema> pipe;
	protected boolean isReading = false;
			
	public ChannelReaderController(Pipe<RawDataSchema> pipe) {
		this.pipe = pipe;
	}
		
	/**
	 * checks if there is data on the channel
	 * @return true if there is a full message to read
	 */
	public boolean hasContentToRead() {
		return Pipe.hasContentToRead(pipe);
	}
	
	/**
	 * Opens the channel reader for reading from beginning of message
	 * @return ChannelReader opened for reading or null if channel has no data
	 */
	public ChannelReader beginRead() {
		if (Pipe.hasContentToRead(pipe)) {
			Pipe.markTail(pipe);
			int msg = Pipe.takeMsgIdx(pipe);
			if (msg >= 0) {
				isReading = true;
				return Pipe.openInputStream(pipe);
			} 
		}
		return null;
	}
	
	/**
	 * Restore position to the beginning, the ChannelReader is invalidated
	 * beginRead() must be called again for another read.
	 */
	public void rollback() {
		if (isReading) {
			Pipe.resetTail(pipe);
		}
		isReading = false;
	}
	
	/**
	 * Move position forward.  ChanelReader is invalid and beginRead() must be called again.
	 */
	public void commitRead() {
		if (isReading) {
			Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(RawDataSchema.instance,RawDataSchema.MSG_CHUNKEDSTREAM_1));
			Pipe.releaseReadLock(pipe);
		}
		isReading = false;
	}

}
