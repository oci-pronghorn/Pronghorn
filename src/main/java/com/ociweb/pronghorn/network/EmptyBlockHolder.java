package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;

public class EmptyBlockHolder {
	
	public long block1PositionOfLen;
	public int block1HeaderBlobPosition;

	public EmptyBlockHolder() {
	}

	public void holdEmptyBlock(
					            long connectionId, 
					            final int sequenceNo,
					            Pipe<ServerResponseSchema> pipe) {
	
			Pipe.addMsgIdx(pipe, ServerResponseSchema.MSG_TOCHANNEL_100);
			Pipe.addLongValue(connectionId, pipe);
			Pipe.addIntValue(sequenceNo, pipe);	
			
			DataOutputBlobWriter<?> outputStream = Pipe.outputStream(pipe);	
			block1HeaderBlobPosition = Pipe.getWorkingBlobHeadPosition(pipe);
	
			DataOutputBlobWriter.openFieldAtPosition(outputStream, block1HeaderBlobPosition); 	//no context, that will come in the second message 
	        
			//for the var field we store this as meta then length
			block1PositionOfLen = (1+Pipe.workingHeadPosition(pipe));
			
			DataOutputBlobWriter.closeLowLevelMaxVarLenField(outputStream);
			assert(pipe.maxVarLen == Pipe.slab(pipe)[((int)block1PositionOfLen) & Pipe.slabMask(pipe)]) : "expected max var field length";
			
			Pipe.addIntValue(0, pipe); //not needed, this is set later
			//the full blob size of this message is very large to ensure we have room later...
			//this call allows for the following message to be written after this messages blob data
			int consumed = Pipe.writeTrailingCountOfBytesConsumed(outputStream.getPipe()); 
			assert(pipe.maxVarLen == consumed);
			Pipe.confirmLowLevelWrite(pipe); 
			//Stores this publish until the next message is complete and published
			Pipe.storeUnpublishedWrites(outputStream.getPipe());
	
			
			//logger.info("new empty block at {} {} ",block1HeaderBlobPosition, block1PositionOfLen);
	}

	public void openToEmptyBlock(DataOutputBlobWriter<?> outputStream) {
		DataOutputBlobWriter.openFieldAtPosition(outputStream, block1HeaderBlobPosition);
	}

	public void finalizeLengthOfFirstBlock(DataOutputBlobWriter<?> outputStream) {
		int propperLength = DataOutputBlobWriter.length(outputStream);
		Pipe.validateVarLength(outputStream.getPipe(), propperLength);
		Pipe.setIntValue(propperLength, outputStream.getPipe(), block1PositionOfLen); //go back and set the right length.
		outputStream.getPipe().closeBlobFieldWrite();
	}
}