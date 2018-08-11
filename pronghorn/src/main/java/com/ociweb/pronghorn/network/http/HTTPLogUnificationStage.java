package com.ociweb.pronghorn.network.http;

import com.ociweb.pronghorn.network.schema.HTTPLogRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPLogResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.ISOTimeFormatterLowGC;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.ElapsedTimeRecorder;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

/**
 * Takes multiple HTTP log requests and responses and turns them into a RawDataSchema for
 * easier output.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class HTTPLogUnificationStage extends PronghornStage {

	private static final byte[] BYTES_RESPONSE = "OUT ".getBytes();
	private static final byte[] BYTES_REQUEST = "IN ".getBytes();
	private static final byte[] BYTES_EOL = "\r\n".getBytes();
	private static final byte[] BYTES_C = "] ".getBytes();
	private static final byte[] BYTES_BUSINESS = "BusinessLatency:".getBytes();
	private static final byte[] BYTES_B = ":".getBytes();
	private static final byte[] BYTES_A = " [".getBytes();
	private final Pipe<HTTPLogRequestSchema>[] requestInputs;
	private final Pipe<HTTPLogResponseSchema>[] responseInputs;			                          
	private final Pipe<RawDataSchema> output;
		
	private ISOTimeFormatterLowGC formatter;
	private ElapsedTimeRecorder etr;

	private boolean messageOpen = false;
	private int cyclesOfNoWork = 0;

	/**
	 *
	 * @param graphManager
	 * @param requestInputs _in_ All HTTP request logs.
	 * @param responseInputs _in_ All HTTP response logs.
	 * @param output _out_ All the request and response logs combined onto the output pipe as a RawDataSchema.
	 */
	public HTTPLogUnificationStage(GraphManager graphManager, 
			                          Pipe<HTTPLogRequestSchema>[] requestInputs,
			                          Pipe<HTTPLogResponseSchema>[] responseInputs,			                          
			                          Pipe<RawDataSchema> output) {
		
		super(graphManager, join(requestInputs, responseInputs), output);
		this.requestInputs = requestInputs;
		this.responseInputs = responseInputs;
		this.output = output;

		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
	}
	
	@Override
	public void startup() {
		etr = new ElapsedTimeRecorder();
		formatter = new ISOTimeFormatterLowGC();
	}

	@Override
	public void run() {
		boolean hasWork = false;
		boolean didWork = false;
		do {
			hasWork = false;
			//all events older or equal to the activeTime are sent
			int i = requestInputs.length;
			while (--i >= 0) {
				if (Pipe.hasRoomForWrite(output)) {
					if (Pipe.peekMsg(requestInputs[i],HTTPLogRequestSchema.MSG_REQUEST_1)) {
						hasWork = true;
						didWork = true;
						logRequestNow(requestInputs[i],output);
					} 
				} else {
					return;//try again later
				}
			}
			
			int j = responseInputs.length;
			while (--j>=0) {
				if (Pipe.hasRoomForWrite(output)) {
					if (Pipe.peekMsg(responseInputs[j],HTTPLogResponseSchema.MSG_RESPONSE_1)) {
						hasWork = true;
						didWork = true;
						logResponseNow(responseInputs[j],output);
					}
				} else {
					return;//try again later
				}
			}			
		} while (hasWork);
				
		if (didWork) {			
			if (this.didWorkMonitor != null) {
				didWorkMonitor.published(); //did work but we are batching.
			}
			cyclesOfNoWork = 0;
		} else {
			//we have nothing else to log to get these out the door.
			if (++cyclesOfNoWork>1000 && messageOpen) {
				DataOutputBlobWriter.closeLowLevelField(Pipe.outputStream(output)); 
				Pipe.confirmLowLevelWrite(output, Pipe.sizeOf(output, RawDataSchema.MSG_CHUNKEDSTREAM_1));
				Pipe.publishWrites(output);	
				messageOpen=false;
			}
		}
		
		checkForShutdown();
		
	}

	int iteration = 0;
	private void checkForShutdown() {
		//don't check for shutdown on every pass it 
		if (0==(0xFFF&iteration++)) {
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
	}

	@Override
	public void shutdown() {
		//finish last write if needed.
		if (messageOpen) {
			DataOutputBlobWriter.closeLowLevelField(Pipe.outputStream(output)); 
			Pipe.confirmLowLevelWrite(output, Pipe.sizeOf(output, RawDataSchema.MSG_CHUNKEDSTREAM_1));
			Pipe.publishWrites(output);	
			messageOpen=false;
		}
	}
	
	private void logResponseNow(Pipe<HTTPLogResponseSchema> p, Pipe<RawDataSchema> output) {

		int msgId = Pipe.takeMsgIdx(p);
		assert(msgId == HTTPLogResponseSchema.MSG_RESPONSE_1);
		
		long timeNS = Pipe.takeLong(p); //time		
		long chnl = Pipe.takeLong(p); //channelId
		int seq = Pipe.takeInt(p);  //sequenceId
		DataInputBlobReader<HTTPLogResponseSchema> header = Pipe.openInputStream(p); //head
		long durationNS = Pipe.takeLong(p);
		

			//batch the writes..
			int esitmate = 100+header.available();
			batchMessages(output, esitmate);
							
			publishLogMessage(timeNS, chnl, seq, durationNS, BYTES_RESPONSE, header, Pipe.outputStream(output));
			

		
		
		Pipe.confirmLowLevelRead(p, Pipe.sizeOf(p, HTTPLogResponseSchema.MSG_RESPONSE_1));
		Pipe.releaseReadLock(p);

	}

	private void logRequestNow(Pipe<HTTPLogRequestSchema> p, Pipe<RawDataSchema> output) {
		
		int msgId = Pipe.takeMsgIdx(p);
		assert(msgId == HTTPLogRequestSchema.MSG_REQUEST_1);
		
		long timeNS = Pipe.takeLong(p); //time
		long chnl = Pipe.takeLong(p); //channelId
		int seq = Pipe.takeInt(p);  //sequenceId
		DataInputBlobReader<HTTPLogRequestSchema> header = Pipe.openInputStream(p); //head
		
			//batch the writes..
			int esitmate = 100+header.available();
			batchMessages(output, esitmate);
			
			publishLogMessage(timeNS, chnl, seq, -1, BYTES_REQUEST, header, Pipe.outputStream(output));
			

		
		Pipe.confirmLowLevelRead(p, Pipe.sizeOf(p, HTTPLogRequestSchema.MSG_REQUEST_1));
		Pipe.releaseReadLock(p);
			
	}

	private void batchMessages(Pipe<RawDataSchema> output, int esitmate) {
		if ((!messageOpen) || (Pipe.outputStream(output).remaining() < (esitmate+(1<<12))) ) {
			if (messageOpen) {
				//add to end of each file, when there is room.
				if (Pipe.outputStream(output).remaining()>(1<<12)) {
					Pipe.outputStream(output).append("\n");
					etr.report(Pipe.outputStream(output));
				}
				
				DataOutputBlobWriter.closeLowLevelField(Pipe.outputStream(output)); 
				Pipe.confirmLowLevelWrite(output, Pipe.sizeOf(output, RawDataSchema.MSG_CHUNKEDSTREAM_1));
				Pipe.publishWrites(output);
				messageOpen=false;
			}
			Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
			Pipe.openOutputStream(output);
			messageOpen=true;
		}
	}
	
	private int etlCounter = 0;
	
	private void publishLogMessage(long timeNS, long chnl, int seq, long duration, 
								   byte[] state,
			                       DataInputBlobReader<?> header,
			                       DataOutputBlobWriter<?> writer) {

		writer.write(state);
		
		writer.write(BYTES_A);
		Appendables.appendHexDigits(writer, chnl); //MAY BE BETTER AS HEX
		writer.write(BYTES_B);
		Appendables.appendHexDigits(writer, seq);  //MAY BE BETTER AS HEX
		writer.write(BYTES_C);

		if (duration>=0) {
			ElapsedTimeRecorder.record(etr, duration);
			writer.write(BYTES_BUSINESS);
			Appendables.appendNearestTimeUnit(writer, duration);
			
			//done every 4K cycles, not need to go faster since we must gather data
			if (((++etlCounter&0xFFF) == 0) && writer.remaining()>(1<<12)) {
				writer.append("\n");
				etr.report(writer);
				//System.err.println(etr);
			}
			
		}
		writer.write(BYTES_EOL);
		
		
		//reconstruct arrival time using the NS time provided
		long arrivalTimeMS = System.currentTimeMillis() - ((System.nanoTime()-timeNS)/1_000_000L);
		
		formatter.write(arrivalTimeMS, writer);
		writer.write(BYTES_EOL);
					
		//This is very fast but the data is not validated,
		//the log file will contain the exact bytes but it may not be
		//readable by normal text editors. This is needed for maximum volume
		header.readInto(writer, header.available());
		writer.write(BYTES_EOL);
		writer.write(BYTES_EOL);

		
	}
	
}
