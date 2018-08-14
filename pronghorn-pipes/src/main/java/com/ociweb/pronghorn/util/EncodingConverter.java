package com.ociweb.pronghorn.util;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.util.TrieParserReader;

public class EncodingConverter {

	private final int MAX_TEXT_LENGTH = 2048;
	private final Pipe<RawDataSchema> workingPipeA = RawDataSchema.instance.newPipe(3,MAX_TEXT_LENGTH);
	private final Pipe<RawDataSchema> workingPipeB = RawDataSchema.instance.newPipe(3,MAX_TEXT_LENGTH);
	private final TrieParserReader templateParserReader = new TrieParserReader(true);

	public interface EncodingStorage {

		void store(Pipe<RawDataSchema> pipe);

	}

	public interface EncodingTransform {

		void transform(TrieParserReader templateParserReader, DataOutputBlobWriter<RawDataSchema> outputStream);

	}
    
    public EncodingConverter() {
    	
    	workingPipeA.initBuffers();
    	workingPipeB.initBuffers();
    }

	private DataOutputBlobWriter<RawDataSchema> openEncoder(Pipe<RawDataSchema> workingPipeOut) {
		Pipe.addMsgIdx(workingPipeOut, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		DataOutputBlobWriter<RawDataSchema> outputStream = Pipe.outputStream(workingPipeOut);
		DataOutputBlobWriter.openField(outputStream);
		return outputStream;
	}

	private static void setupParser(TrieParserReader templateParserReader, Pipe<RawDataSchema> workingPipeIn) {
		Pipe.takeMsgIdx(workingPipeIn);
		TrieParserReader.parseSetup(templateParserReader, workingPipeIn);
	}

	private static void convertUTF8ToBytes(Pipe<RawDataSchema> workingPipeIn, CharSequence route) {
		Pipe.addMsgIdx(workingPipeIn, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		Pipe.addUTF8(route, workingPipeIn);
		Pipe.confirmLowLevelWrite(workingPipeIn);
		Pipe.publishWrites(workingPipeIn);
	}

	public void convert(CharSequence textBlock, EncodingTransform transform, EncodingStorage storage) {
		
		cleanInternalState();
		DataOutputBlobWriter<RawDataSchema> outputStream = openTransform(textBlock);
		transform.transform(templateParserReader, outputStream);
		closeTransform(outputStream);
		storage.store(workingPipeB);
		closeStore();
		
	}

	private void closeStore() {
		Pipe.confirmLowLevelWrite(workingPipeB);
		Pipe.releaseReadLock(workingPipeB);
	}

	private void closeTransform(DataOutputBlobWriter<RawDataSchema> outputStream) {
		//finished writing the converted bytes
		closeEncoder(workingPipeB, outputStream);		
		//release the input template
		Pipe.releaseReadLock(workingPipeA);
		Pipe.takeMsgIdx(workingPipeB);
	}

	private DataOutputBlobWriter<RawDataSchema> openTransform(CharSequence textBlock) {
		convertUTF8ToBytes(workingPipeA, textBlock);		
		setupParser(templateParserReader, workingPipeA);
		DataOutputBlobWriter<RawDataSchema> outputStream = openEncoder(workingPipeB);
		return outputStream;
	}

	private void cleanInternalState() {
		workingPipeA.reset();
		workingPipeB.reset();
	} 

	private void closeEncoder(Pipe<RawDataSchema> workingPipeOut, DataOutputBlobWriter<RawDataSchema> outputStream) {
		DataOutputBlobWriter.closeLowLevelField(outputStream);
		Pipe.confirmLowLevelWrite(workingPipeOut);
		Pipe.publishWrites(workingPipeOut);
	}
}
