package com.ociweb.logtelemetry;

import java.util.ArrayList;
import java.util.List;

import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.http.HTTPUtil;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.TrieParserReaderLocal;

public class LogTelemetryRestStage extends PronghornStage {

	final Pipe<HTTPRequestSchema>[] inputPipes;
	final Pipe<ServerResponseSchema>[] outputs; 
	final Pipe<RawDataSchema> logFile;
	
	public LogTelemetryRestStage(GraphManager graphManager, 
			Pipe<HTTPRequestSchema>[] inputPipes,
			Pipe<ServerResponseSchema>[] outputPipes,
			Pipe<RawDataSchema> logFile) {
		super(graphManager,join(inputPipes,logFile),outputPipes);
		
		this.inputPipes = inputPipes;
		this.outputs = outputPipes;
		this.logFile = logFile;
	}

	private TrieParser lineParser;
	private TrieParserReader reader;
	private DataInputBlobReader<RawDataSchema> logFileInput;
	private List<byte[]> files;

	private int logIndex = 0;
	
	@Override
	public void startup() {
		
		//load log file and parse out all the telemetry snapshots...
		
		lineParser = new TrieParser();
		lineParser.setUTF8Value("digraph gl {\n", 1);
		lineParser.setUTF8Value("}\n", 2);
		lineParser.setUTF8Value("%b\n", 3);
		
		reader = TrieParserReaderLocal.get();
		
		logFileInput = Pipe.inputStream(logFile);	
		
	   
			//start
//		[8:RProxy] INFO com.ociweb.pronghorn.stage.scheduling.GraphManager - 
//		20181212215745711.dot
//		digraph gl {
			//body
			//stop
//      }		
	
	}
	
	private StringBuilder accum;
	
	@Override
	public void run() {
		if (null==logFileInput) {//already loaded the file so start taking requests
			//System.out.println("images:"+files.size());
			
			int i = inputPipes.length;
			while(--i>=0) {
				process(inputPipes[i], outputs[i]);
			}
		} else {
			//still loading the file
			
			if (null == files) {
				TrieParserReader.parseSetup(reader, logFileInput);
				files = new ArrayList<byte[]>();
			}
			
			int len;
			while ( (len = readNext()) > 0  ) {
				
				reader.sourceLen += len; //todo odd..

				
				long startLen = reader.sourceLen;
				long id;
				while ( (id=reader.parseNext(lineParser))>=0 ) {
		
					if (id == 1) {					    
						accum = new StringBuilder();
						accum.append("digraph gl {\n");						
						//System.out.println("start new fiel");
					}
					if (id == 2) {
						accum.append("}\n");
						files.add(accum.toString().getBytes());
						accum = null;
						//System.out.println("added new file");
					}
					if (id == 3) {
						if (null!=accum) {
							TrieParserReader.capturedFieldBytesAsUTF8(reader, 0, accum);	
							accum.append('\n');
						}						
					}
				}
				
				Pipe.releasePendingAsReadLock(logFile, (int)(startLen-reader.sourceLen));

			}
			
			if (len<0) {
				//done, ready for traffic.
				logFileInput = null;
			}
			
		}
		
	}

	//TODO: needs to be a general pattern???
	private int readNext() {
		if (Pipe.hasContentToRead(logFile)) {

			int idx = Pipe.takeMsgIdx(logFile);
			if (idx>=0) {
		
				int len = DataInputBlobReader.accumLowLevelAPIField(logFileInput);
				Pipe.confirmLowLevelRead(logFile, Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1));
				Pipe.readNextWithoutReleasingReadLock(logFile);
				return len;
			} else {
				System.out.println("end detected");
				Pipe.confirmLowLevelRead(logFile, Pipe.EOF_SIZE);
				Pipe.readNextWithoutReleasingReadLock(logFile);
				return -1;
			}
		} else {
			return 0;
		}
	}

	
	
	private void process(Pipe<HTTPRequestSchema> input, 
			             Pipe<ServerResponseSchema> output) {
		
//		if (!Pipe.hasContentToRead(input) && !Pipe.isEmpty(input)) {
//			System.out.println(input);
//		}
//		
		while (Pipe.hasContentToRead(input) && Pipe.hasRoomForWrite(output)) {
			
		    int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		        case HTTPRequestSchema.MSG_RESTREQUEST_300:
		        	
					long fieldChannelId = Pipe.takeLong(input);
					int fieldSequence = Pipe.takeInt(input);
					int fieldVerb = Pipe.takeInt(input);
					DataInputBlobReader<HTTPRequestSchema> data = Pipe.openInputStream(input);
					int fieldRevision = Pipe.takeInt(input);
					int fieldRequestContext = Pipe.takeInt(input);
					
					int channelIdHigh = (int)(fieldChannelId>>32); 
					int channelIdLow = (int)fieldChannelId;		
					
					//rotate over each of the images..

					byte[] contentBacking = files.get(logIndex);
					int contentLength = contentBacking.length;
					
					//System.out.println("msg Idx, sending file "+logIndex+" of len "+contentLength);

					if (++logIndex>=files.size()) {
						logIndex=0;
					}
					int contentPosition = 0;
					int contentMask = Integer.MAX_VALUE;
					
					HTTPUtil.publishArrayResponse(ServerCoordinator.END_RESPONSE_MASK,
							      fieldSequence, 200, output, channelIdHigh, channelIdLow,
							      HTTPContentTypeDefaults.DOT.getBytes(),
							      contentLength, contentBacking, contentPosition, contentMask);

			        Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
			        Pipe.releaseReadLock(input);				
		            
		        break;
		        case -1:
			        Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
			        Pipe.releaseReadLock(input);
		        	//System.out.println("got EOF shutdown");
		        	Pipe.publishEOF(output);
		        break;
		        default:
		        	
		        	System.out.println("unsupported value: "+msgIdx);
		        	
		    }
		}
	}
	
}
