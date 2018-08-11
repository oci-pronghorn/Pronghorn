package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.util.ByteConsumer;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.parse.JSONVisitor;

public class OAuth2BearerExtractor implements JSONVisitor {

	private final String typeValueKey      = "token_type";
	private final String typeValueExpected = "bearer";
	private final String typeAccessKey     = "access_token";
	
	private final TrieParser keyParser = new TrieParser(128,1,false,false);
	private final TrieParser valueParser = new TrieParser(128,1,false,false);
	private final TrieParserReader reader = new TrieParserReader();
	
	private final PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance,4, 256);
	private final Pipe<RawDataSchema> buffer = new Pipe<RawDataSchema>(config);
	
	private long lastKeyToken = -1;
	private boolean bearerKeyFound = false;
	
	private final StringBuilder result = new StringBuilder();
	
	
	public OAuth2BearerExtractor() {
		buffer.initBuffers();
		
		keyParser.setUTF8Value(typeValueKey, 1);
		keyParser.setUTF8Value(typeAccessKey, 2);
		
		valueParser.setUTF8Value(typeValueExpected, 3);
		
	}
	
	public CharSequence getBearer() {
		if (!bearerKeyFound) {
			return null;
		}
		
		return result;
	}
	
	public void reset() {
		bearerKeyFound=false;
		lastKeyToken = -1;
		result.setLength(0);
	}

	public boolean hasBearer() {
		return bearerKeyFound;
	}
	
	@Override
	public ByteConsumer stringValue() {
		
		Pipe.addMsgIdx(buffer, RawDataSchema.MSG_CHUNKEDSTREAM_1);		
		DataOutputBlobWriter<RawDataSchema> writer = Pipe.outputStream(buffer);
		DataOutputBlobWriter.openField(writer);
		return writer;
	}

	@Override
	public void stringValueComplete() {
		
		/////////////////////////////////////////////////////////
		
		DataOutputBlobWriter.closeLowLevelField(Pipe.outputStream(buffer));
		Pipe.confirmLowLevelWrite(buffer, Pipe.sizeOf(RawDataSchema.instance,  RawDataSchema.MSG_CHUNKEDSTREAM_1));
		Pipe.publishWrites(buffer);
		
		/////////////////////////////////////////////////////////
		
  		if (lastKeyToken == 2) {
  		
  			Pipe.takeMsgIdx(buffer);
  			
  			
  			//store this value for use
  			result.setLength(0);
			DataInputBlobReader<RawDataSchema> localReader = Pipe.inputStream(buffer);
			localReader.openLowLevelAPIField();
			
			localReader.readUTFOfLength(localReader.available(), result);

			Pipe.confirmLowLevelRead(buffer, Pipe.sizeOf(RawDataSchema.instance,  RawDataSchema.MSG_CHUNKEDSTREAM_1));
			Pipe.releaseReadLock(buffer);

  		} else {
		
			Pipe.takeMsgIdx(buffer);
			TrieParserReader.parseSetup(reader, buffer);		
			long value = TrieParserReader.parseNext(reader, valueParser);
	  		Pipe.confirmLowLevelRead(buffer, Pipe.sizeOf(RawDataSchema.instance,  RawDataSchema.MSG_CHUNKEDSTREAM_1));
	  		Pipe.releaseReadLock(buffer);
	  		  		
	  		//////////////////////////////////////////////////////////
	 		
	  		if (lastKeyToken == 1 && value==3) {  		
	  			bearerKeyFound = true;
	  		}
  		}

  		
  		
	}

	@Override
	public ByteConsumer stringName(int fieldIdx) {
		Pipe.addMsgIdx(buffer, RawDataSchema.MSG_CHUNKEDSTREAM_1);		
		DataOutputBlobWriter<RawDataSchema> writer = Pipe.outputStream(buffer);
		DataOutputBlobWriter.openField(writer);
		return writer;
	}

	@Override
	public void stringNameComplete() {
		
		//////////////////////////////////////////////////////////
		
		DataOutputBlobWriter.closeLowLevelField(Pipe.outputStream(buffer));
		Pipe.confirmLowLevelWrite(buffer, Pipe.sizeOf(RawDataSchema.instance,  RawDataSchema.MSG_CHUNKEDSTREAM_1));
		Pipe.publishWrites(buffer);
		
		/////////////////////////////////////////////////////////
		
		Pipe.takeMsgIdx(buffer);
		TrieParserReader.parseSetup(reader, buffer);		
		lastKeyToken = TrieParserReader.parseNext(reader, keyParser);
  		Pipe.confirmLowLevelRead(buffer, Pipe.sizeOf(RawDataSchema.instance,  RawDataSchema.MSG_CHUNKEDSTREAM_1));
  		Pipe.releaseReadLock(buffer);
  		
		///////////////////////////////////////////////////////////

  		 		
  		
  		
  		
	}

	@Override
	public void arrayBegin() {
		throw new UnsupportedOperationException("not expected in this request");		
	}

	@Override
	public void arrayEnd() {
		throw new UnsupportedOperationException("not expected in this request");
	}

	@Override
	public void arrayIndexBegin(int instance) {
		throw new UnsupportedOperationException("not expected in this request");
	}

	@Override
	public void numberValue(long m, byte e) {
		throw new UnsupportedOperationException("not expected in this request");
	}

	@Override
	public void nullValue() {
		throw new UnsupportedOperationException("not expected in this request");
	}

	@Override
	public void booleanValue(boolean b) {
		throw new UnsupportedOperationException("not expected in this request");
	}

	@Override
	public void objectEnd() {
		
	}

	@Override
	public void objectBegin() {
		
	}



}
