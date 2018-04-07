package com.ociweb.pronghorn.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.StructuredReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.TrieParserReaderLocal;
import com.ociweb.pronghorn.util.parse.JSONStreamParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public class HTTPRequestJSONExtractionStage extends PronghornStage {

	private final JSONExtractorCompleted extractor;
	private int[] indexPositions;
	
	private final Pipe<HTTPRequestSchema> input;
	private final Pipe<HTTPRequestSchema> output;
		
	private JSONStreamParser parser;
	private JSONStreamVisitorToChannel visitor;
	private final StructRegistry typeData;
	private final int structId;
	
	public static final Logger logger = LoggerFactory.getLogger(HTTPRequestJSONExtractionStage.class);
		
	//TODO: add error response pipe however we first need to update graph building to pass this in.
	public HTTPRequestJSONExtractionStage(GraphManager graphManager, 
											JSONExtractorCompleted extractor,  int structId,
											Pipe<HTTPRequestSchema> input,
											Pipe<HTTPRequestSchema> output) {
		
		super(graphManager, input, output);
		this.extractor = extractor;
		this.input = input;
		this.output = output;
		this.typeData = graphManager.recordTypeData;
		this.structId = structId;
		
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
		
	}

	@Override
	public void startup() {

		parser = new JSONStreamParser();
		visitor = extractor.newJSONVisitor();
		indexPositions = extractor.indexTable(typeData, structId);
		
	}
	
	@Override
	public void run() {
		
		final TrieParserReader reader = TrieParserReaderLocal.get();
				
		Pipe<HTTPRequestSchema> localInput = input;
		Pipe<HTTPRequestSchema> localOutput = output;
		
		while (Pipe.hasContentToRead(localInput) && Pipe.hasRoomForWrite(localOutput) ) {
			
		    int msgIdx = Pipe.takeMsgIdx(localInput);
		   
		    switch(msgIdx) {
		        case HTTPRequestSchema.MSG_RESTREQUEST_300:
		        {
		        	long channelId = Pipe.takeLong(localInput);
		        	int sequenceNum = Pipe.takeInt(localInput);
		        	int verb = Pipe.takeInt(localInput);      	
		        	
		        	DataInputBlobReader<HTTPRequestSchema> inputStream = Pipe.openInputStream(localInput);
	       			        	
		        	int payloadOffset = inputStream.readFromEndLastInt(StructuredReader.PAYLOAD_INDEX_LOCATION);
		        			        		        	
		        	DataOutputBlobWriter<HTTPRequestSchema> outputStream = Pipe.openOutputStream(localOutput);
		        	inputStream.readInto(outputStream, payloadOffset);//copies params and headers.
		    
		        	//inputStream is now positioned to the JSON
		        	//outputStream is now positions as the target
		        	reader.parseSetup(inputStream);
		    		parser.parse(reader, extractor.trieParser(), visitor);
		    		
		    		//if (TrieParserReader.parseHasContent(reader)) {
		    		//	logger.info("calls detected with {} bytes after JSON.",TrieParserReader.parseHasContentLength(reader));
		    		//}
		    		
		    		if (!visitor.isReady() ) {
		    			
		    			final int size = Pipe.addMsgIdx(localOutput, msgIdx);
		    			Pipe.addLongValue(channelId, localOutput); //channel
		    			Pipe.addIntValue(sequenceNum, localOutput); //sequence
		    			Pipe.addIntValue(verb, localOutput); //verb
		    			
		    			//parser is not "ready for data" and requires export to be called
		    			visitor.export(outputStream, indexPositions);		
		    			//moves the index data as is
		    			inputStream.readFromEndInto(outputStream);
		    			DataOutputBlobWriter.closeLowLevelField(outputStream);
		    			
		    			Pipe.addIntValue(Pipe.takeInt(localInput), localOutput); //revision
		    			Pipe.addIntValue(Pipe.takeInt(localInput), localOutput); //context
		    			
		    			Pipe.confirmLowLevelWrite(localOutput,size);
		    			Pipe.publishWrites(localOutput);
		    		} else {
		    			//send404 code!
		    			
		    			//parser wants more data or the data is not understood, eg broken
		    			
		    			//TODO: send 404
		    			//DataOutputBlobWriter.closeLowLevelField(outputStream);
		    			localOutput.closeBlobFieldWrite();
		    			
		    			
		    		}
		        }	
				break;
		        case HTTPRequestSchema.MSG_WEBSOCKETFRAME_100:
		        {	
		        	long channelId = Pipe.takeLong(localInput);
		        	int sequenceNum = Pipe.takeInt(localInput);
		        	int finOpp = Pipe.takeInt(localInput);
		        	int maskVal = Pipe.takeInt(localInput);
		        	
		        	DataInputBlobReader<HTTPRequestSchema> inputStream = Pipe.openInputStream(localInput);
					
		        	assert(inputStream.isStructured()) : "Structured stream is required for JSON";
		        	assert(DataInputBlobReader.getStructType(inputStream) == structId) : "Only supports one JSON Strcture at a time.";
		        
		        	int payloadOffset = inputStream.readFromEndLastInt(StructuredReader.PAYLOAD_INDEX_LOCATION);
		    		        			        	
		        	int size = Pipe.addMsgIdx(localOutput, msgIdx);
		        	Pipe.addLongValue(channelId, localOutput); //channel
		        	Pipe.addIntValue(sequenceNum, localOutput); //sequence
		        	Pipe.addIntValue(finOpp, localOutput); //FinOpp
		        	Pipe.addIntValue(maskVal, localOutput); //Mask
		        	DataOutputBlobWriter<HTTPRequestSchema> outputStream = Pipe.openOutputStream(localOutput);
		        	inputStream.readInto(outputStream, payloadOffset);//copies params and headers.
				    
		        	//inputStream is now positioned to the JSON
		        	//outputStream is now positions as the target
		        	reader.parseSetup(inputStream);
		        			    		
		    		//for each block? as it goes for post? TODO: do later...
		    		parser.parse(reader, extractor.trieParser(), visitor);
		        			    
		    		if (visitor.isReady()) {
		    			//needs more data.
		    			
		    		} else {	
		    			
		    		}
		    		
		    		//TODO: may need multiple of these for streaming..
		    		visitor.export(outputStream, indexPositions);		
		    		
		    		//moves the index data as is
		        	inputStream.readFromEndInto(outputStream); //TODO: only needed on first block
		        					    
				    Pipe.confirmLowLevelWrite(localOutput,size);
				    Pipe.publishWrites(localOutput);
		        }
		        break;
		        case -1:
		           requestShutdown();
		        break;
		    }
		    Pipe.confirmLowLevelRead(localInput, Pipe.sizeOf(HTTPRequestSchema.instance, msgIdx));
		    Pipe.releaseReadLock(localInput);
		    			
			
		}
						
	}

}
