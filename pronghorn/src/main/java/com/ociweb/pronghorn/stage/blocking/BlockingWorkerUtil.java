package com.ociweb.pronghorn.stage.blocking;

import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.StructuredReader;

public class BlockingWorkerUtil { //move to PH

	static final int SIZE_OF = Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1);

	public static <T> void processAssociatedOperator(Pipe<RawDataSchema> input, T that, Pipe<RawDataSchema> output) {
		///////////////////////////////////////////////////////////////
		
		int msgIdx = Pipe.takeMsgIdx(input);			
		ChannelReader inStream = Pipe.openInputStream(input);
		assert(inStream.isStructured());
	
	    ///////////////////////////////////////////////////////////////
	
		int size = Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		ChannelWriter outStream = Pipe.openOutputStream(output);
							
		BlockingWorkerOperation<T> operation = inStream.structured().structAssociatedObject();
		
		//copy over these two fields without having to decode encode them.
		assert( inStream.structured().associatedFieldObject(StructuredReader.structType(inStream.structured()), 0).toString().equalsIgnoreCase("connection"));
		assert( inStream.structured().associatedFieldObject(StructuredReader.structType(inStream.structured()), 1).toString().equalsIgnoreCase("sequence"));	
		int copyLimit = inStream.structured().dataPositionFromIndexOffset(2);
		assert(inStream.structured().dataPositionFromIndexOffset(0)<copyLimit) : "Connection must appear before business fields";
		assert(inStream.structured().dataPositionFromIndexOffset(1)<copyLimit) : "Sequence must appear before business fields";
					
		inStream.readInto(outStream, copyLimit);
		((DataInputBlobReader<?>)inStream).readFromEndInto((DataOutputBlobWriter<?>) outStream);			
		DataOutputBlobWriter.setStructType((DataOutputBlobWriter<?>)outStream,-1); //clear out copied type
		//////////////////////
		
		operation.execute(inStream, that, outStream);	
		
		outStream.closeLowLevelField();
		
		Pipe.confirmLowLevelWrite(output, size);
		Pipe.publishWrites(output);
				
		///////////////////////////////////////////////
		//release last after we publish the results, this is key to ensure we can see the data on the graph.
		Pipe.confirmLowLevelRead(input, SIZE_OF);
		Pipe.releaseReadLock(input);
	}

}
