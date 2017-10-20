package com.ociweb.pronghorn.stage.network;

import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import org.junit.*;

public class SocketClientTestDataStage extends PronghornStage {

	private final Pipe<NetPayloadSchema>[] input; 
	private final Pipe<ReleaseSchema> releasePipe;
	private final boolean encrytpedContent;
	private final int testUsers;
	private final int[] testSeeds;
	private final int[] testSizes;
	private int expectedCountRemaining;
	private int[] bytesPerUser;
	private int expectedBytesPerUser;
		
	public SocketClientTestDataStage(GraphManager gm, Pipe<NetPayloadSchema>[] input, Pipe<ReleaseSchema> releasePipe, 
			                    boolean encryptedContent, int testUsers, int[] testSeeds, int[] testSizes) {
		super(gm,input,releasePipe);
		this.input = input;
		this.releasePipe = releasePipe;
		this.encrytpedContent = encryptedContent;
		this.testUsers=testUsers;
		this.testSeeds=testSeeds;
		this.testSizes=testSizes;
	}

	@Override
	public void startup() {
		
		this.bytesPerUser = new int[testUsers+1]; //tge id values are 1 base offset
				
		int sum = 0;
		//count all the bytes..
		int i = testSizes.length;
		while (--i>=0) {
			sum += testSizes[i];		
		}
		expectedBytesPerUser = sum;
		this.expectedCountRemaining = testUsers*expectedBytesPerUser;
				
	}
	
	@Override
	public void shutdown() {
		
		Assert.assertEquals(0, bytesPerUser[0]);
		for(int x=1;x<bytesPerUser.length;x++) { 
			Assert.assertEquals(expectedBytesPerUser, bytesPerUser[x]);
		}
		
	}
	
	@Override
	public void run() {
		
		int i = input.length;
		while (--i>=00) {
			int count = testNewData(input[i],releasePipe, encrytpedContent,bytesPerUser);
			expectedCountRemaining-=count;
			if (expectedCountRemaining==0 || count<0) {
				requestShutdown();
				return;
			}
		}
	}

	private static int testNewData(Pipe<NetPayloadSchema> pipe, Pipe release, boolean encrytpedContent, int[] bytesPerUser) {
		
		int count = 0;
		while (Pipe.hasContentToRead(pipe) && Pipe.hasRoomForWrite(release)) {
			
			switch (Pipe.takeMsgIdx(pipe)) {
			
				case NetPayloadSchema.MSG_DISCONNECT_203:
					throw new UnsupportedOperationException("Not expected in test");
					//break;
				case NetPayloadSchema.MSG_ENCRYPTED_200:
					{
					Assert.assertTrue(encrytpedContent);
					long conId = Pipe.takeLong(pipe);
					long arrivalTime = Pipe.takeLong(pipe);
					
					int meta = Pipe.takeRingByteMetaData(pipe);
					int len = Pipe.takeRingByteLen(pipe);		
					int pos = Pipe.bytePosition(meta, pipe, len);
					
					//TODO: add test here for payload
					
					Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_ENCRYPTED_200));
					Pipe.releaseReadLock(pipe);
					
					publishRelease(release, conId, Pipe.tailPosition(pipe));
					
					bytesPerUser[(int)conId] += len;
					
					count += len;
					}
					break;
				case NetPayloadSchema.MSG_PLAIN_210:
					{
					Assert.assertFalse(encrytpedContent);
					
					long conId = Pipe.takeLong(pipe);
					long arrivalTime = Pipe.takeLong(pipe);
					long position = Pipe.takeLong(pipe);
					

					int meta = Pipe.takeRingByteMetaData(pipe);
					int len = Pipe.takeRingByteLen(pipe);					
					int pos = Pipe.bytePosition(meta, pipe, len);
					

					//TODO: add test here for payload
					
					Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210));
					Pipe.releaseReadLock(pipe);
					
					publishRelease(release, conId, position>=0?position:Pipe.tailPosition(pipe));
					
					bytesPerUser[(int)conId] += len;
					
					count += len;
					}	
					break;
				case -1:
					Pipe.confirmLowLevelRead(pipe, Pipe.EOF_SIZE);
					Pipe.releaseReadLock(pipe);
					return -1;
				case NetPayloadSchema.MSG_BEGIN_208:
					
					int seq = Pipe.takeInt(pipe);					
					Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(NetPayloadSchema.instance,NetPayloadSchema.MSG_BEGIN_208));
					Pipe.releaseReadLock(pipe);
					
					break;
			}	
		}
		return count;
		
	}

	private static void publishRelease(Pipe pipe, long conId, long position) {
		assert(position!=-1);
		int size = Pipe.addMsgIdx(pipe, ReleaseSchema.MSG_RELEASE_100);
		Pipe.addLongValue(conId, pipe);
		Pipe.addLongValue(position, pipe);
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
	}
	
	

}
