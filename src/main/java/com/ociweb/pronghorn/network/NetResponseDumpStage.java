package com.ociweb.pronghorn.network;

import java.io.IOException;

import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class NetResponseDumpStage<A extends Appendable> extends PronghornStage {

	private final Pipe<NetResponseSchema> input;
	private final A target;
	
	public NetResponseDumpStage(GraphManager graphManager, Pipe<NetResponseSchema> input, A target) {
		super(graphManager, input, NONE);
		this.input = input;
		this.target = target;
	}

	@Override
	public void run() {
		
		while(Pipe.hasContentToRead(input)) {
			
			int id = Pipe.takeMsgIdx(input);
			switch (id) {
				case NetResponseSchema.MSG_RESPONSE_101:
					{
						long connection = Pipe.takeLong(input);
						
						DataInputBlobReader<NetResponseSchema> stream = Pipe.inputStream(input);
						stream.openLowLevelAPIField();
						
						int status = stream.readShort();
						System.out.println("status:"+status);
						
						int headerId = stream.readShort();
						
						while (-1 != headerId) { //end of headers will be marked with -1 value
							//determine the type
							
							int headerValue = stream.readShort();
							//is this what we need?
							System.out.println(headerId+"  "+headerValue);
							
							
							//read next
							headerId = stream.readShort();
							
						}
						System.out.println("last short:"+headerId);
						
						try {
							DataInputBlobReader.readUTF(stream, stream.available(), target);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, id));
						Pipe.releaseReadLock(input);
					}	
					
					break;
				case NetResponseSchema.MSG_CONTINUATION_102:
					{
						System.out.println("XXXXXXXXX continuation");
						
						long connection = Pipe.takeLong(input);
						
						DataInputBlobReader<NetResponseSchema> stream = Pipe.inputStream(input);
						stream.openLowLevelAPIField();
						
						//NOTE: how do we know to remove the headers??
						stream.readUTF(target);
						
						Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, id));
						Pipe.releaseReadLock(input);
					}
					
					break;
					
				case NetResponseSchema.MSG_CLOSED_10:
					
					Pipe.takeRingByteMetaData(input);
					Pipe.takeRingByteLen(input);
					Pipe.takeInt(input);
					
					Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, id));
					Pipe.releaseReadLock(input);
					
					break;
				case -1:
					Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
					Pipe.releaseReadLock(input);
					requestShutdown();
					return;
			    default:
			        throw new UnsupportedOperationException("not yet implemented support for "+id);	 
			
			}
		
			
			
			
		}
		
		
	}

}
