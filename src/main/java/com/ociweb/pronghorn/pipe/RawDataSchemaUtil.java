package com.ociweb.pronghorn.pipe;

public class RawDataSchemaUtil {

	public static boolean accumulateInputStream(Pipe<RawDataSchema> pipe) {
		int msgIdx = Pipe.takeMsgIdx(pipe);
		boolean isEnd;
		if (RawDataSchema.MSG_CHUNKEDSTREAM_1 == msgIdx) {
			isEnd = DataInputBlobReader.accumLowLevelAPIField(Pipe.inputStream(pipe))<=0;
			Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
		} else {
			//eof
			isEnd = true;
			Pipe.confirmLowLevelRead(pipe, Pipe.EOF_SIZE);
		}
		Pipe.readNextWithoutReleasingReadLock(pipe);
		return isEnd;
	}

	public static void releaseConsumed(Pipe<RawDataSchema> pipe, ChannelReader reader, int startingAvail) {
		//release the number of bytes we consumed
		Pipe.releasePendingAsReadLock(pipe, startingAvail-reader.available());
	}

}
