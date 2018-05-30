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

/**
 * Parses a JSON response using a JSONStreamVisitor.
 * @param <M>
 * @param <T>
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
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

	/**
	 *
 	 * @param graphManager
	 * @param input _in_ The NetResponseSchema containing the JSON to be parsed.
	 * @param bottom
	 * @param visitor
	 */
	public NetResponseJSONStage(GraphManager graphManager, Pipe<NetResponseSchema> input, int bottom, JSONStreamVisitor visitor) {
		super(graphManager, input, NONE);
		this.input = input;
		this.visitor = visitor;
		this.output = null;
		this.keys = null;
		this.mapper = null;
		this.bottomOfJSON = bottom;
	}

	/**
	 *
	 * @param graphManager
	 * @param keys
	 * @param mapper
	 * @param input _in_ The NetResponseSchema containing the JSON to be parsed.
	 * @param bottom
	 * @param output _out_ Put the parsed JSON onto this output pipe.
	 * @param otherOutputs _out_ Put the parsed JSON onto multiple output pipes.
	 */
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

		if (0==reader.sourceLen) {
			//now that reader is empty this is when we can release all the released reads
			while (Pipe.releasePendingCount(input)>0) {
				Pipe.releasePendingReadLock(input);//release 1
			}
		}
		
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
						
						//logger.info("old reader value should be zero was "+reader.sourceLen);
						
						//skip over all the headers, no need to read them at this time
						DataInputBlobReader.position(stream, stream.readFromEndLastInt(PAYLOAD_INDEX_OFFSET));
						DataInputBlobReader.setupParser(stream, reader);	
					
						//logger.info("new reader now has "+reader.sourceLen+" vs "+stream.available());
						
	
					}	
					
					break;
				case NetResponseSchema.MSG_CONTINUATION_102:
					{
						
						//logger.info("reading continuation");
						
						long connection = Pipe.takeLong(input);
						int flags2 = Pipe.takeInt(input);
		            	 
						//TODO: header payload has extra space at the end we must not assume is good!
						
						if (reader.sourceLen==0) {
														
							DataInputBlobReader<NetResponseSchema> stream = Pipe.openInputStream(input);
					   	    DataInputBlobReader.setupParser(stream, reader);
							//logger.info("reading new data of length "+reader.sourceLen+" "+stream.available());
						} else {
							//logger.info("adding more data current total is "+reader.sourceLen);
							int meta = Pipe.takeRingByteMetaData(input);
							int len = Pipe.takeRingByteLen(input);
							int pos = Pipe.bytePosition(meta, input, len);//must call for side effect
							
							//copy up the remaining data to make it a single block.
							//due to this copy we must down below ensure that we keep both reads unreleased
							final int targetPos =  reader.sourceMask & (input.sizeOfBlobRing+(pos-reader.sourceLen));
							Pipe.copyBytesFromToRing(Pipe.blob(input), reader.sourcePos, reader.sourceMask, 
									                 Pipe.blob(input), targetPos, reader.sourceMask, 
									                 reader.sourceLen);
							//////////////////////////////
							
							if (len>0) {
								reader.sourceLen += len;
								assert(reader.sourceLen <= input.sizeOfBlobRing) : "added "+len+" and total "+reader.sourceLen+" is larger than "+input.sizeOfBlobRing;
							}
							
							reader.sourcePos = targetPos;
							
							//NOTE: we are still using the last part of the previous record so it must
							//      not be written over until this is consumed
														
							//logger.info("adding "+len+" new bytes total is now "+reader.sourceLen);
						}
					}
					
					break;
					
				case NetResponseSchema.MSG_CLOSED_10:
				
					//logger.info("connection closed");
					
					int meta = Pipe.takeRingByteMetaData(input); //host
					int len  = Pipe.takeRingByteLen(input); //host
					int pos = Pipe.bytePosition(meta, input, len);
					byte[] backing = Pipe.blob(input);
					int mask = Pipe.blobMask(input);
										
					
					int port = Pipe.takeInt(input); //port
					
					processCloseEvent(backing, pos, len, mask, port);
					
					break;
				case -1:
				
					//logger.info("shutdown detected");
					
					Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
					Pipe.releaseReadLock(input);
					requestShutdown();
					return;
					
					
			}
			
			Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, id));
			Pipe.readNextWithoutReleasingReadLock(input);
			
			assert(input.config().minimumFragmentsOnPipe()>2) : "input pipe must be large enought to hold 2 reads open while new writes continue";
			
			while (Pipe.releasePendingCount(input)>2) {//must always hold open 2 for the rollover.
				Pipe.releasePendingReadLock(input); //release 1
			}
			
		}
		
		//only call when we got a response to send down stream (response can be empty)
		if (gotResponse) {
			if (TrieParserReader.parseHasContent(reader)) {
				//logger.info("parse response");
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
