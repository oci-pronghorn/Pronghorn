package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieKeyable;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.parse.JSONStreamParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitor;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToPipe;
import com.ociweb.pronghorn.util.parse.MapJSONToPipeBuilder;

public class NetResponseJSONStage<M extends MessageSchema<M>, T extends Enum<T>& TrieKeyable> extends PronghornStage {

	private final Pipe<NetResponseSchema> input;
	private JSONStreamVisitor visitor;
	private TrieParserReader reader;
	
	private JSONStreamParser parser = new JSONStreamParser();
	private TrieParser customParser;
	private final Class<T> keys;
	private final Pipe output;
	private final MapJSONToPipeBuilder<M,T> mapper;
	
	public NetResponseJSONStage(GraphManager graphManager, Pipe<NetResponseSchema> input, JSONStreamVisitor visitor) {
		super(graphManager, input, NONE);
		this.input = input;
		this.visitor = visitor;
		this.keys = null;
		this.output = null;
		this.mapper = null;
	}
	
	public NetResponseJSONStage(GraphManager graphManager, Class<T> keys,  MapJSONToPipeBuilder<M,T> mapper, Pipe<NetResponseSchema> input, Pipe<M> output, Pipe ... otherOutputs) {
		super(graphManager, input, join(otherOutputs, output));
		this.input = input;
		this.keys = keys;	
		this.output = output;
		this.mapper = mapper;
	}

	@Override
	public void startup() {
		
		reader = JSONStreamParser.newReader();
		
		if (null!=keys) {						
			this.visitor = new JSONStreamVisitorToPipe(output, keys, mapper);
			this.customParser = JSONStreamParser.customParser(keys);	
		}
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
						int headerId = stream.readShort();
						
						while (-1 != headerId) { //end of headers will be marked with -1 value
							//determine the type
							
							int headerValue = stream.readShort();
							//TODO: add support for other header data.
							
							//read next
							headerId = stream.readShort();
							
						}
												
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
					
					int meta = Pipe.takeRingByteMetaData(input); //host
					int len  = Pipe.takeRingByteLen(input); //host
					int pos = Pipe.bytePosition(meta, input, len);
					byte[] backing = Pipe.blob(input);
					int mask = Pipe.blobMask(input);
										
					
					int port = Pipe.takeInt(input); //port
					
					processCloseEvent(backing, pos, len, mask, port);
										
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

	protected void processCloseEvent(byte[] hostBacking, int hostPos, int hostLen, int hostMask, int port) {
		//default behavior does nothing
	}

}
