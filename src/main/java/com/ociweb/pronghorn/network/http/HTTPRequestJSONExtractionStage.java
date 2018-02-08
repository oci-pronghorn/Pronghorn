package com.ociweb.pronghorn.network.http;

import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.parse.JSONStreamParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public class HTTPRequestJSONExtractionStage extends PronghornStage {

	private final JSONExtractorCompleted extractor;
	private final Pipe<HTTPRequestSchema> input;
	private final Pipe<HTTPRequestSchema> output;
		
	private TrieParserReader reader;
	private JSONStreamParser parser;
	private JSONStreamVisitorToChannel visitor;
		
	
	
	public HTTPRequestJSONExtractionStage(GraphManager graphManager, 
											JSONExtractorCompleted extractor,
											Pipe<HTTPRequestSchema> input,
											Pipe<HTTPRequestSchema> output) {
		
		super(graphManager, input, output);
		this.extractor = extractor;
		this.input = input;
		this.output = output;
		
	}

	@Override
	public void startup() {

		reader = new TrieParserReader(5,true);
		parser = new JSONStreamParser();
		visitor = extractor.newJSONVisitor();
			
	}
	
	@Override
	public void run() {
		
		while (Pipe.hasContentToRead(input) && Pipe.hasRoomForWrite(output) ) {
			
		    int msgIdx = Pipe.takeMsgIdx(input);
		    Pipe.addMsgIdx(output, msgIdx);
		    switch(msgIdx) {
		        case HTTPRequestSchema.MSG_RESTREQUEST_300:
		        {
		        	Pipe.addLongValue(Pipe.takeLong(input), output); //channel
		        	Pipe.addIntValue(Pipe.takeInt(input), output); //sequence
		        	Pipe.addIntValue(Pipe.takeInt(input), output); //verb
		        	
		        	DataInputBlobReader<HTTPRequestSchema> inputStream = Pipe.openInputStream(input);        	
		        	int payloadOffset = inputStream.readFromEndLastInt(1);
		        	assert(inputStream.absolutePosition()>=payloadOffset);
		        	
		        	DataOutputBlobWriter<HTTPRequestSchema> outputStream = Pipe.openOutputStream(output);
		        	inputStream.readInto(outputStream, payloadOffset);//copies params and headers.
		    
		        	//inputStream is now positioned to the JSON
		        	//outputStream is now positions as the target
		        	reader.parseSetup(inputStream);
		    		parser.parse(reader, extractor.trieParser(), visitor);
		    		visitor.export(outputStream);		
		    		
		    		//moves the index data as is
		        	inputStream.readFromEndInto(outputStream);
		        	DataOutputBlobWriter.closeLowLevelField(outputStream);
		        	
		        	//TODO: can we add field name indexes beyond the existing??
		        	//   we need to walk the JSON given the extractor
		        	//   we need the position of the last index, so we need the full block.
		        	
		        	//TODO: first step is add api reqireing use to call 
		        	//      for each field in order
		        	
		        	
		        	
		        	
					Pipe.addIntValue(Pipe.takeInt(input), output); //revision
		        	Pipe.addIntValue(Pipe.takeInt(input), output); //context
		        }	
				break;
		        case HTTPRequestSchema.MSG_WEBSOCKETFRAME_100:
		        {	
		        	Pipe.addLongValue(Pipe.takeLong(input), output); //channel
		        	Pipe.addIntValue(Pipe.takeInt(input), output); //sequence
		        	Pipe.addIntValue(Pipe.takeInt(input), output); //FinOpp
		        	Pipe.addIntValue(Pipe.takeInt(input), output); //Mask
		        	
		        	DataInputBlobReader<HTTPRequestSchema> inputStream = Pipe.openInputStream(input);
		        	int payloadOffset = inputStream.readFromEndLastInt(1);
		        	assert(inputStream.absolutePosition()>=payloadOffset);
		        	
		        	DataOutputBlobWriter<HTTPRequestSchema> outputStream = Pipe.openOutputStream(output);
		        	inputStream.readInto(outputStream, payloadOffset);//copies params and headers.
				    
		        	//inputStream is now positioned to the JSON
		        	//outputStream is now positions as the target
		        	reader.parseSetup(inputStream);
		        			    		
		    		//for each block? as it goes for post? TODO: do later...
		    		parser.parse(reader, extractor.trieParser(), visitor);
		        			    
		    		//TODO: may need multiple of these for streaming..
		    		visitor.export(outputStream);		
		    		
		    		//moves the index data as is
		        	inputStream.readFromEndInto(outputStream); //TODO: only needed on first block
		        	
		        }
		        break;
		        case -1:
		           requestShutdown();
		        break;
		    }
		    int size = Pipe.sizeOf(HTTPRequestSchema.instance, msgIdx);
		    
		    Pipe.confirmLowLevelWrite(output,size);
		    Pipe.publishWrites(output);
		    
		    Pipe.confirmLowLevelRead(input, size);
		    Pipe.releaseReadLock(input);
		    
			
			
		}
		
		
		//HTTPRequestSchema.consume(input);
		
		
		
		//NOTE: we assume the last index is always for the payload and we have nothing after it.
		//DataOutputBlobWriter.countOfBytesUsedByIndex(writer)// min 4 is the body position??
		//setPositionBytesFromStart(readFromEndLastInt(payloadIndexOffset));
		
		
		/////////////////
		
		//take all data from input and write to output
		//except extract data from the JSON.
		
		//payload postion
//        builder.routeHeaderToPositionTable(routeId), 
//         builder.routeExtractionParserIndexCount(routeId),
//		this.payloadIndexOffset = paraIndexCount + IntHashTable.count(headerHash) + 1;
		
        // add field forthe position of the payload?
		// add second field just for payload
		
		//JSON now? add it to open topic? run in place??
		//could do in router.
		  
		//1. is there an easy way to find this? position 
		//2. compute in line and do faster integration.
		//2. begin conversion to add the new fields
		
		
	}

}
