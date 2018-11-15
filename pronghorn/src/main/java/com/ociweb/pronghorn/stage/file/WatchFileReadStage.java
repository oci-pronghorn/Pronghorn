package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.FolderWatchSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class WatchFileReadStage extends PronghornStage {

	private final Pipe<?>[] inputs;
	private final Pipe<RawDataSchema>[] outputs;
	private final Pipe<?>[] ackRelease;   
	private final ReadState[] readState;
	private final Logger logger = LoggerFactory.getLogger(WatchFileReadStage.class);
	
	public static WatchFileReadStage newInstance(GraphManager graphManager, 
            Pipe<?>[] inputs, 
            Pipe<RawDataSchema>[] outputs,
            Pipe<?>[] ackRelease
			) {
		return new WatchFileReadStage(graphManager,inputs, outputs, ackRelease );
	}
	
	
	protected WatchFileReadStage(GraphManager graphManager, 
			                     Pipe<?>[] inputs, //requests for new files to read, must contain path in blob data unstructured.
			                     Pipe<RawDataSchema>[] outputs, //may be encrypted.
			                     Pipe<?>[] ackRelease //downstream is responsible for timeout triggers.
			                     //ack release will cause a -1 to be sent to the associated output to complete the round trip
								) {
		super(graphManager, join(inputs,ackRelease), outputs);
		
		assert(inputs.length == outputs.length);
		assert(outputs.length == ackRelease.length);		
		
		this.inputs = inputs;
		this.outputs = outputs;
		this.ackRelease = ackRelease;
		
		int i = inputs.length;
		this.readState = new ReadState[i];
		while (--i>=0) {
			this.readState[i] = new ReadState();
		}
		
	}

	@Override
	public void run() {
		int i = inputs.length;
		while (--i>=0) {
			process(inputs[i],outputs[i],ackRelease[i],readState[i]);			
		}
	}

	private static void process(Pipe<?> request, Pipe<RawDataSchema> dataOut, Pipe<?> release, ReadState state) {
		if (Pipe.hasRoomForWrite(dataOut)) {
		
			if (Pipe.hasContentToRead(release)) {
				Pipe.skipNextFragment(release);
				Pipe.publishEOF(dataOut);			
				state.clear();				
			}
			
			if (Pipe.hasContentToRead(request)) {
				
				//stop reading if new request comes in
				//and if it is at the end of the read
				//without this check multiple files may start but never get to the end.
				if (state.isReading() && state.isAtEnd()) {
					Pipe.publishEOF(dataOut);			
					state.clear();				
				}
			
				if (!state.isReading()) {
					Pipe.takeMsgIdx(request);
					DataInputBlobReader<?> inputStream = Pipe.inputStream(request);
					
					inputStream.openLowLevelAPIField(); //path in the stream or all if this is raw
					if (Pipe.isForSchema(request, FolderWatchSchema.class)) {
						inputStream.accumLowLevelAPIField(); //name in the stream
					}
					
					state.beginFileRead(inputStream.readUTF());
				}
			}
			
			
			state.fromFileToPipe(dataOut);
			
			
			
			
		}
	}
	
	private static final transient FileSystem fileSystem = FileSystems.getDefault();
	
	class ReadState {
		
		private InputStream inputStream;

		public boolean isReading() {
			return null != inputStream;
		}
		
		public boolean isAtEnd() {
			try {
				return null==inputStream || inputStream.available()<=0;
			} catch (IOException e) {
				//ignore
				return true;
			}
		}

		public void clear() {
			try {
				inputStream.close();
			} catch (IOException e) {
				//ignore
			}
			inputStream=null;
		}

		public void beginFileRead(String absolutePath) {
			       
			Path path = fileSystem.getPath(absolutePath);		    
		    try {
				inputStream = fileSystem.provider().newInputStream(path, StandardOpenOption.READ);				
			} catch (IOException e) {
				logger.warn("File error ",e);
				inputStream = null;				
			}
		    
		}

		public void fromFileToPipe(Pipe<RawDataSchema> dataOut) {
			if (null!=inputStream) {
				try {
					//must move this many bytes if we have room
					int avail = inputStream.available();
					while (avail>0 && Pipe.hasRoomForWrite(dataOut)) {
						final int maxCopy = Math.min(avail, dataOut.maxVarLen);
						int size = Pipe.addMsgIdx(dataOut, RawDataSchema.MSG_CHUNKEDSTREAM_1);
						Pipe.readFieldFromInputStream(dataOut, inputStream, maxCopy);
						Pipe.confirmLowLevelWrite(dataOut, size);
						Pipe.publishWrites(dataOut);												
						avail -= maxCopy;
					}
				
				} catch (IOException e) {
					logger.warn("File error ",e);
					inputStream = null;
				}
			}
			if (null == inputStream) { ///may be from here or from beginFileRead..
				Pipe.publishEOF(dataOut);
			}			
		}		
				
	}

}
