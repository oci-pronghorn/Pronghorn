package com.ociweb.pronghorn.network.http;

import com.ociweb.pronghorn.network.schema.HTTPLogRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPLogResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.ISOTimeFormatterLowGC;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

public class HTTPLogUnificationStage extends PronghornStage {

	private long activeTime = 0;
	private final Pipe<HTTPLogRequestSchema>[] requestInputs;
	private final Pipe<HTTPLogResponseSchema>[] responseInputs;			                          
	private final Pipe<RawDataSchema> output;
		
	private ISOTimeFormatterLowGC formatter;
	
	protected HTTPLogUnificationStage(GraphManager graphManager, 
			                          Pipe<HTTPLogRequestSchema>[] requestInputs,
			                          Pipe<HTTPLogResponseSchema>[] responseInputs,			                          
			                          Pipe<RawDataSchema> output) {
		
		super(graphManager, join(requestInputs, responseInputs), output);
		this.requestInputs = requestInputs;
		this.responseInputs = responseInputs;
		this.output = output;
		
	}
	
	@Override
	public void startup() {
		formatter = new ISOTimeFormatterLowGC();

	}

	@Override
	public void run() {
		boolean hasWork = false;
		
		do {
			//consume the messages off the input pipes but we want them in time order
			long lowestFoundTime = Long.MAX_VALUE;
			hasWork = false;
			//all events older or equal to the activeTime are sent
			int i = requestInputs.length;
			while (--i>=0) {
				Pipe<HTTPLogRequestSchema> p = requestInputs[i];
				if (Pipe.peekMsg(p,HTTPLogRequestSchema.MSG_REQUEST_1)) {				
					long time = Pipe.peekLong(p, 0xFF&HTTPLogRequestSchema.MSG_REQUEST_1_FIELD_TIME_11);
					if (time<=activeTime) {
						if (Pipe.hasRoomForWrite(output)) {
							logRequestNow(p,output);
						} else {
							return;//try again later
						}
					} else {
						hasWork = true;
						if (time<=lowestFoundTime) {
							//keep this for the next pass
							lowestFoundTime = time;
						}
					}
				} 
			}
			int j = responseInputs.length;
			while (--j>=0) {
				Pipe<HTTPLogResponseSchema> p = responseInputs[j];
				if (Pipe.peekMsg(p,HTTPLogResponseSchema.MSG_RESPONSE_1)) {
					long time = Pipe.peekLong(p, 0xFF&HTTPLogResponseSchema.MSG_RESPONSE_1_FIELD_TIME_11);
					if (time<=activeTime) {
						if (Pipe.hasRoomForWrite(output)) {
							logResponseNow(p,output);
						} else {
							return;//try again later
						}
					} else {
						hasWork = true;
						if (time<=lowestFoundTime) {
							//keep this for the next pass
							lowestFoundTime = time;
						}
					}
				}
			}
			//at the end of the loop activeTime is set to lowestFoundTime
			activeTime = lowestFoundTime;
			
		} while (hasWork);
		
		checkForShutdown();
		
	}

	private void checkForShutdown() {
		///////we have no work and we are not blocked.
		//must check for shutdown case
		int i = requestInputs.length;
		while (--i>=0) {
			Pipe<HTTPLogRequestSchema> p = requestInputs[i];
			if (!Pipe.peekMsg(p,-1)) {
				return;//this one was not shut down yet
			}			
		}
		int j = responseInputs.length;
		while (--j>=0) {
			Pipe<HTTPLogResponseSchema> p = responseInputs[j];
			if (!Pipe.peekMsg(p,-1)) {
				return;//this one was not shut down yet
			}			
		}
		//all the pipes have now requested shutdown
		if (Pipe.hasRoomForWrite(output)) {
			Pipe.publishEOF(output); //tell downstream to shutdown
			requestShutdown(); //shutdown myself			
		} else {
			return;//try again later
		}
	}

	private void logResponseNow(Pipe<HTTPLogResponseSchema> p, Pipe<RawDataSchema> output) {

		int msgId = Pipe.takeMsgIdx(p);
		assert(msgId == HTTPLogResponseSchema.MSG_RESPONSE_1);
		
		long time = Pipe.takeLong(p); //time
		long chnl = Pipe.takeLong(p); //channelId
		int seq = Pipe.takeInt(p);  //sequenceId
		DataInputBlobReader<HTTPLogResponseSchema> header = Pipe.openInputStream(p); //head
		
		int size = Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		DataOutputBlobWriter<?> writer = Pipe.openOutputStream(output);
						
		publishLogMessage(time, chnl, seq, "Request", header, writer);
		
		DataOutputBlobWriter.closeLowLevelField((DataOutputBlobWriter<?>) writer); //TODO: odd signature
		Pipe.confirmLowLevelWrite(output, size);
		Pipe.publishWrites(output);
		
		//will be needed for the payload support later
		Pipe.takeInt(p); //contentType

	}

	private void logRequestNow(Pipe<HTTPLogRequestSchema> p, Pipe<RawDataSchema> output) {
		
		int msgId = Pipe.takeMsgIdx(p);
		assert(msgId == HTTPLogRequestSchema.MSG_REQUEST_1);
		
		long time = Pipe.takeLong(p); //time
		long chnl = Pipe.takeLong(p); //channelId
		int seq = Pipe.takeInt(p);  //sequenceId
		DataInputBlobReader<HTTPLogRequestSchema> header = Pipe.openInputStream(p); //head
		
		int size = Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		DataOutputBlobWriter<?> writer = Pipe.openOutputStream(output);
		
		publishLogMessage(time, chnl, seq, "Response", header, writer);
		
		DataOutputBlobWriter.closeLowLevelField((DataOutputBlobWriter<?>) writer); //TODO: odd signature
		Pipe.confirmLowLevelWrite(output, size);
		Pipe.publishWrites(output);
		
		//will be needed for the payload support later
		Pipe.takeInt(p); //contentType
			
	}
	
	private void publishLogMessage(long time, long chnl, int seq, CharSequence state,
			                       DataInputBlobReader<?> header, DataOutputBlobWriter<?> writer) {

		formatter.write(time, writer);
		writer.write(" [0x".getBytes());
		Appendables.appendHexDigitsRaw(writer, chnl);
		writer.write(":0x".getBytes());
		Appendables.appendHexDigitsRaw(writer, seq);
		writer.write("] ".getBytes());
		writer.append(state);
		writer.write("\r\n".getBytes());
				
		//use read as UTF and fully convert to ensure that the data is readable.
		try {
			DataInputBlobReader.readUTF(header, header.available(), writer);
		} catch (Exception e) {
			//The header was not proper UTF8 so mark this in the log file at that point
			writer.write("<NOT-UTF8>\r\n\r\n".getBytes());
		}
		
	}
	
}
