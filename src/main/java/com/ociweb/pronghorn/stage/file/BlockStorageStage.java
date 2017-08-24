package com.ociweb.pronghorn.stage.file;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageReceiveSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageXmitSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class BlockStorageStage extends PronghornStage {

	private final String filePath;
	private final Pipe<BlockStorageXmitSchema>[] input; 
	private final Pipe<BlockStorageReceiveSchema>[] output;
	private RandomAccessFile raf;
	private FileChannel fileChannel;
	private int shutdownCountdown;
	
	
	public static BlockStorageStage newInstance(GraphManager graphManager, 
			                             String filePath, //single file accessed by multiple pipes
							             Pipe<BlockStorageXmitSchema>[] input, 
							             Pipe<BlockStorageReceiveSchema>[] output) {
		return new BlockStorageStage(graphManager, filePath, input, output);
	}
	
	public static BlockStorageStage newInstance(GraphManager graphManager, 
							             String filePath, //single file accessed by multiple pipes
							             Pipe<BlockStorageXmitSchema> input, 
							             Pipe<BlockStorageReceiveSchema> output) {
		return new BlockStorageStage(graphManager, filePath, 
				                     new Pipe[]{input}, 
				                     new Pipe[]{output});
	}
	
	public BlockStorageStage(GraphManager graphManager, 
			                    String filePath, //single file accessed by multiple pipes
			                    Pipe<BlockStorageXmitSchema>[] input, 
			                    Pipe<BlockStorageReceiveSchema>[] output) {
		
		super(graphManager, input, output);
		
		this.filePath = filePath;
		this.input = input;
		this.output = output;
	}

	@Override
	public void startup() {
		try {
			shutdownCountdown = input.length;
			raf = new RandomAccessFile(filePath, "rws");
			fileChannel = raf.getChannel();
		} catch (FileNotFoundException e) {
			new RuntimeException(e);
		}
	}
	
	@Override
	public void run() {
		if (0 == shutdownCountdown) {
			int j = output.length;
			while (--j>=0) {
				if (!PipeWriter.hasRoomForWrite(output[j])) {
					return;
				}
			}
			PipeWriter.publishEOF(output);
			requestShutdown();
			return;
		}
		
		int i = input.length;
		while (--i >= 0) {			
			if (PipeWriter.hasRoomForWrite(output[i])) {
				processRequest(input[i], output[i]);
			}
		}
	}

	private void processRequest(Pipe<BlockStorageXmitSchema> input, 
			                    Pipe<BlockStorageReceiveSchema> output) {
		
		while (PipeReader.tryReadFragment(input)) {
		    int msgIdx = PipeReader.getMsgIdx(input);
		    switch(msgIdx) {
		        case BlockStorageXmitSchema.MSG_WRITE_1:
		            
		        	final long fieldPosition = PipeReader.readLong(input,BlockStorageXmitSchema.MSG_WRITE_1_FIELD_POSITION_12);
		            
					try {
						fileChannel.position(fieldPosition);						
						ByteBuffer[] buffers = PipeReader.wrappedUnstructuredLayoutBuffer(input, BlockStorageXmitSchema.MSG_WRITE_1_FIELD_PAYLOAD_11);
						long wrote = fileChannel.write(buffers);
						
						BlockStorageReceiveSchema.publishWriteAck(output, fieldPosition);
						
					} catch (IOException e) {						
						BlockStorageReceiveSchema.publishError(output, fieldPosition, e.getMessage());	
					}
		        break;
		        case BlockStorageXmitSchema.MSG_READ_2:
		            
		        	
		        		final long fieldPosition1 = PipeReader.readLong(input,BlockStorageXmitSchema.MSG_READ_2_FIELD_POSITION_12);
		        		final int readLength = PipeReader.readInt(input,BlockStorageXmitSchema.MSG_READ_2_FIELD_READLENGTH_10);
		        		assert(readLength>0) : "found value "+readLength+" file read must be a postitive value.";
		        	
		        	
					try {
						assert(readLength>0);
						fileChannel.position(fieldPosition1);

						ByteBuffer[] target = PipeWriter.wrappedUnstructuredLayoutBufferOpen(output, readLength,
								                         BlockStorageReceiveSchema.MSG_DATARESPONSE_1_FIELD_PAYLOAD_11);
						
						//may be -1 for end of file
						int readLen = (int)fileChannel.read(target);
						
						
						PipeWriter.presumeWriteFragment(output, BlockStorageReceiveSchema.MSG_DATARESPONSE_1);
						
						PipeWriter.wrappedUnstructuredLayoutBufferClose(output, 
								BlockStorageReceiveSchema.MSG_DATARESPONSE_1_FIELD_PAYLOAD_11, readLength);
						
						
						PipeWriter.writeLong(output,
								BlockStorageReceiveSchema.MSG_DATARESPONSE_1_FIELD_POSITION_12,
								fieldPosition1);
						PipeWriter.publishWrites(output);
												
					} catch (IOException e) {
						PipeWriter.wrappedUnstructuredLayoutBufferCancel(output);
						
						BlockStorageReceiveSchema.publishError(output, fieldPosition1, e.getMessage());	
					}
		        break;
		        case -1:
		        	shutdownCountdown--;		           
		        break;
		    }
		    PipeReader.releaseReadLock(input);
		}
	}

}
