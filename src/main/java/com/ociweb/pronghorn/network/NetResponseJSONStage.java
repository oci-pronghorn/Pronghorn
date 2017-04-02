package com.ociweb.pronghorn.network;

import java.io.IOException;

import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.parse.JSONStreamParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitor;

public class NetResponseJSONStage extends PronghornStage {

	private final Pipe<NetResponseSchema> input;
	private final JSONStreamVisitor visitor;
	private TrieParserReader reader;
	
	private JSONStreamParser parser = new JSONStreamParser();
	private final TrieParser customParser;
	
	public NetResponseJSONStage(GraphManager graphManager, Pipe<NetResponseSchema> input, JSONStreamVisitor visitor) {
		super(graphManager, input, NONE);
		this.input = input;
		this.visitor = visitor;
		this.customParser = null;
	}
	
	public NetResponseJSONStage(GraphManager graphManager, Pipe<NetResponseSchema> input, JSONStreamVisitor visitor, TrieParser customParser) {
		super(graphManager, input, NONE);
		this.input = input;
		this.visitor = visitor;
		this.customParser = customParser;
	}

	@Override
	public void startup() {
		
		reader = JSONStreamParser.newReader();
		
	}
	
	
	@Override
	public void run() {

		
		while(Pipe.hasContentToRead(input)) {
			
			int id = Pipe.takeMsgIdx(input);
			switch (id) {
				case NetResponseSchema.MSG_RESPONSE_101:
					{
						long connection = Pipe.takeLong(input);
						
						DataInputBlobReader<NetResponseSchema> stream = Pipe.inputStream(input);
						DataInputBlobReader.openLowLevelAPIField(stream);
						
						int status = stream.readShort();
						System.out.println("status:"+status);
						
						int headerId = stream.readShort();
						
						while (-1 != headerId) { //end of headers will be marked with -1 value
							//determine the type
							
							int headerValue = stream.readShort();
							//is this what we need?
							System.out.println(headerId+"  "+headerValue);
							
							
							//read next
							headerId = stream.readShort();
							
						}
						System.out.println("last short:"+headerId);
												
						DataInputBlobReader.setupParser(stream, reader);
	
						
						Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, id));
						Pipe.releaseReadLock(input);
					}	
					
					break;
				case NetResponseSchema.MSG_CONTINUATION_102:
					{
						long connection = Pipe.takeLong(input);
						
						if (reader.sourceLen==0) {
						
							DataInputBlobReader<NetResponseSchema> stream = Pipe.inputStream(input);
							DataInputBlobReader.openLowLevelAPIField(stream);
							DataInputBlobReader.setupParser(stream, reader);
							
						} else {
							
							Pipe.takeRingByteMetaData(input);
							int len = Pipe.takeRingByteLen(input);
							reader.sourceLen += len;
							
						}
						
						Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, id));
						Pipe.releaseReadLock(input);
					}
					
					break;
					
				case NetResponseSchema.MSG_CLOSED_10:
					
					Pipe.takeRingByteMetaData(input);
					Pipe.takeRingByteLen(input);
					Pipe.takeInt(input);
					
					Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, id));
					Pipe.releaseReadLock(input);
					
					break;
				case -1:
					Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
					Pipe.releaseReadLock(input);
					requestShutdown();
					return;
						
			}
					
			
		}
		
		if (null!=customParser) {
			parser.parse(reader, customParser, visitor);			
		} else {
			parser.parse(reader, visitor);
		}
		
		
		
		
	}

}
