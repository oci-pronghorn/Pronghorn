package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorCapture;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToPipe;
import com.ociweb.pronghorn.util.parse.MapJSONToPipeBuilder;

public class NetResponseJSONStage<M extends MessageSchema<M>, T extends Enum<T>& TrieKeyable> extends PronghornStage {

	private final Pipe<NetResponseSchema> input;
	private JSONStreamVisitor visitor;
	private TrieParserReader reader;
	
	private JSONStreamParser parserJSON = new JSONStreamParser();
	private TrieParser customParser;
	protected final Class<T> keys;
	protected final Pipe<M> output;
	protected final MapJSONToPipeBuilder<M,T> mapper;
	
	private static final Logger logger = LoggerFactory.getLogger(NetResponseJSONStage.class);
	private static final int PAYLOAD_INDEX_OFFSET = 1;
	protected final int bottomOfJSON;
	
	public NetResponseJSONStage(GraphManager graphManager, Pipe<NetResponseSchema> input, int bottom, JSONStreamVisitor visitor) {
		super(graphManager, input, NONE);
		this.input = input;
		this.visitor = visitor;
		this.output = null;
		this.keys = null;
		this.mapper = null;
		this.bottomOfJSON = bottom;
	}
	
	public NetResponseJSONStage(GraphManager graphManager, Class<T> keys,  MapJSONToPipeBuilder<M,T> mapper, Pipe<NetResponseSchema> input, int bottom, Pipe<M> output, Pipe ... otherOutputs) {
		super(graphManager, input, join(otherOutputs, output));
		this.input = input;
		this.output = output;
		this.bottomOfJSON = bottom;
		
		boolean debug = false;
		
		this.keys = debug ? null : keys;	
		this.mapper = debug ? null : mapper;
		
		if (debug) {
			this.visitor = new JSONStreamVisitorCapture(System.out);
		}
		
	}

	@Override
	public void startup() {
		
		reader = JSONStreamParser.newReader();
		
		if (null!=keys) {						
			this.visitor = buildVisitor();

			this.customParser = JSONStreamParser.customParser(keys);	
		}
	}

	protected JSONStreamVisitorToPipe buildVisitor() {
		return new JSONStreamVisitorToPipe(output, keys, bottomOfJSON, mapper);
	}
	
	@Override
	public void run() {

		boolean gotResponse = false;
		while(Pipe.hasContentToRead(input)) {
			gotResponse = true;
			final int id = Pipe.takeMsgIdx(input);
			switch (id) {
				case NetResponseSchema.MSG_RESPONSE_101:
					{
						//logger.info("reading response");
						
						long connection = Pipe.takeLong(input);
						int flags = Pipe.takeInt(input);
						 
						DataInputBlobReader<NetResponseSchema> stream = Pipe.inputStream(input);
						stream.openLowLevelAPIField();
						
						//System.out.println("length  is "+stream.available()+" vs max of "+input.maxVarLen);
						
						int status = stream.readShort();
						//System.err.println("got response with status "+status);
						
						//skip over all the headers, no need to read them at this time
						stream.setPositionBytesFromStart(stream.readFromEndLastInt(PAYLOAD_INDEX_OFFSET));
					
						DataInputBlobReader.setupParser(stream, reader);	
						
	
					}	
					
					break;
				case NetResponseSchema.MSG_CONTINUATION_102:
					{
						
						//logger.info("reading continuation");
						
						long connection = Pipe.takeLong(input);
						int flags2 = Pipe.takeInt(input);
		            	 
						if (reader.sourceLen==0) {
							
							DataInputBlobReader<NetResponseSchema> stream = Pipe.inputStream(input);
							stream.openLowLevelAPIField();
							DataInputBlobReader.setupParser(stream, reader);
							
						} else {
							
							Pipe.takeRingByteMetaData(input);
							int len = Pipe.takeRingByteLen(input);
							if (len>0) {
								reader.sourceLen += len;
								assert(reader.sourceLen <= input.sizeOfBlobRing) : "added "+len+" and total "+reader.sourceLen+" is larger than "+input.sizeOfBlobRing;
							}
						}
						
					}
					
					break;
					
				case NetResponseSchema.MSG_CLOSED_10:
					
					logger.info("reading closed");
					
					int meta = Pipe.takeRingByteMetaData(input); //host
					int len  = Pipe.takeRingByteLen(input); //host
					int pos = Pipe.bytePosition(meta, input, len);
					byte[] backing = Pipe.blob(input);
					int mask = Pipe.blobMask(input);
										
					
					int port = Pipe.takeInt(input); //port
					
					processCloseEvent(backing, pos, len, mask, port);
					
					break;
				case -1:
					logger.info("shutdown detected ");
					Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
					Pipe.releaseReadLock(input);
					requestShutdown();
					return;
			}
			
			Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, id));
			Pipe.releaseReadLock(input);
		}
		
		//only call when we got a response to send down stream (response can be empty)
		if (gotResponse) {
			if (TrieParserReader.parseHasContent(reader)) {
				if (null!=customParser) {
					parserJSON.parse(reader, customParser, visitor);			
				} else {
					parserJSON.parse(reader, visitor);
				}
			}
		
			//logger.info("finished block");
			//must call with postId to outer class but how to capture??
			finishedBlock();
		}
		
	}

	protected void finishedBlock() {
		//default behavior does nothing
	}
	
	protected void processCloseEvent(byte[] hostBacking, int hostPos, int hostLen, int hostMask, int port) {
		//default behavior does nothing
	}

}
