package com.ociweb.pronghorn.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.StructuredReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.TrieParserReaderLocal;
import com.ociweb.pronghorn.util.parse.JSONStreamParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

/**
 * Using a JSONExtractor, takes a HTTP request with JSON and turns it into
 * a ServerResponseSchema onto a ServerResponseSchema pipe.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class NetResponseJSONExtractionStage extends PronghornStage {
	
	private final JSONExtractorCompleted extractor;
	private int[] indexPositions;
	
	private final Pipe<NetResponseSchema> input;
	private final Pipe<NetResponseSchema> output;

	private JSONStreamParser parser;
	private JSONStreamVisitorToChannel visitor;

	public static final Logger logger = LoggerFactory.getLogger(HTTPRequestJSONExtractionStage.class);

	/**
	 *
	 * @param graphManager
	 * @param extractor
	 * @param structId
	 * @param input _in_ The HTTP request containing JSON.
	 * @param output _out_ The HTTP response.
	 */
	public NetResponseJSONExtractionStage(GraphManager graphManager, 
											JSONExtractorCompleted extractor, 
											Pipe<NetResponseSchema> input,
											Pipe<NetResponseSchema> output) {
		
		super(graphManager, input, output);
		this.extractor = extractor;
		this.input = input;
		this.output = output;

		//modify the struct to add the JSON fields
		indexPositions = extractor.getIndexPositions();
		
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
		
	}

	@Override
	public void startup() {

		parser = new JSONStreamParser();
		visitor = extractor.newJSONVisitor();
			
	}
		
	@Override
	public void run() {
		
		final TrieParserReader reader = TrieParserReaderLocal.get();
				
		Pipe<NetResponseSchema> localInput = input;
		Pipe<NetResponseSchema> localOutput = output;
		
		while (Pipe.hasContentToRead(localInput) && Pipe.hasRoomForWrite(localOutput) ) {
			
		    int msgIdx = Pipe.takeMsgIdx(localInput);
		   
		    switch(msgIdx) {
		    
		    	case NetResponseSchema.MSG_RESPONSE_101:
		        {
		        	//hold to pass forward
		        	long conId = Pipe.takeLong(localInput);
		        	int contextFlags = Pipe.takeInt(localInput);
		        	DataInputBlobReader<NetResponseSchema> inputStream = Pipe.openInputStream(localInput); //payload
	       			        	
		        	//copies params and headers.
		        	DataOutputBlobWriter<NetResponseSchema> outputStream = Pipe.openOutputStream(localOutput);
		        	inputStream.readInto(outputStream, inputStream.readFromEndLastInt(StructuredReader.PAYLOAD_INDEX_LOCATION));
		    
		        	//inputStream is now positioned to the JSON
		        	//outputStream is now positions as the target
		        	DataInputBlobReader.setupParser(inputStream, reader);
		    		parser.parse(reader, extractor.trieParser(), visitor);
		    		
		    		//if (TrieParserReader.parseHasContent(reader)) {
		    		//	logger.info("calls detected with {} bytes after JSON.",TrieParserReader.parseHasContentLength(reader));
		    		//}
		    		
		    		if (!visitor.isReady() ) {
		    			
		    			final int size = Pipe.addMsgIdx(localOutput, msgIdx);
		    			
		    			Pipe.addLongValue(conId, localOutput);
		    			Pipe.addIntValue(contextFlags, localOutput);
		    			
		    			//moves the index data as is and must happen before JSON updates index
		    			inputStream.readFromEndInto(outputStream);
		    			//parser is not "ready for data" and requires export to be called
		    			//this expoert will populate the index positinos for the JSON fields

		    			visitor.export(outputStream, indexPositions);
		    			DataOutputBlobWriter.commitBackData(outputStream, extractor.getStructId());
		    			
		    			DataOutputBlobWriter.closeLowLevelField(outputStream);
				    			
		    			Pipe.confirmLowLevelWrite(localOutput,size);
		    			Pipe.publishWrites(localOutput);
		    		} else {
		    			//send what data we have
		    			logger.debug("Unable to parse JSON");		    			
		    			
	    			    final int size = Pipe.addMsgIdx(localOutput, msgIdx);
		    			
		    			Pipe.addLongValue(conId, localOutput);
		    			Pipe.addIntValue(contextFlags, localOutput);
		    			
		    			//moves the index data as is and must happen before JSON updates index
		    			inputStream.readFromEndInto(outputStream);
		    			//parser is not "ready for data" and requires export to be called
		    			//this expoert will populate the index positinos for the JSON fields

		    			//do not call export since the JSON was broken
		    			///visitor.export(outputStream, indexPositions);
		    			
		    			DataOutputBlobWriter.commitBackData(outputStream, extractor.getStructId());
		    			
		    			DataOutputBlobWriter.closeLowLevelField(outputStream);
				    			
		    			Pipe.confirmLowLevelWrite(localOutput,size);
		    			Pipe.publishWrites(localOutput);
		    			
		    			visitor.clear();//rest for next JSON
		    		}
		        }	
		        break;
		    	case NetResponseSchema.MSG_CONTINUATION_102:
		    		throw new UnsupportedOperationException("Support for JSON spanning frames in the response is not yet implemented.");
		    	case NetResponseSchema.MSG_CLOSED_10:
		    		final int size = Pipe.addMsgIdx(localOutput, msgIdx);
		    		 
		    		ChannelReader hostReader = Pipe.openInputStream(localInput);		    		
		    		ChannelWriter hostWriter = Pipe.openOutputStream(localOutput);
		    		hostReader.readInto(hostWriter, hostReader.available());
		    		hostWriter.closeLowLevelField();
		    		
		    		Pipe.addIntValue(Pipe.takeInt(localInput), localOutput);
		    		
	    			Pipe.confirmLowLevelWrite(localOutput,size);
	    			Pipe.publishWrites(localOutput);
			    	break;
			    	
		        case -1:
		           requestShutdown();
		        break;
		    }
		    Pipe.confirmLowLevelRead(localInput, Pipe.sizeOf(localInput, msgIdx));
		    Pipe.releaseReadLock(localInput);
		}			
	}

}
