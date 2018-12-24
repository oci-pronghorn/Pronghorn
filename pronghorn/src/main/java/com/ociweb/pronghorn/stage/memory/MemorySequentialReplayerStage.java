package com.ociweb.pronghorn.stage.memory;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadConsumerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadProducerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadReleaseSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreConsumerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreProducerSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MemorySequentialReplayerStage extends PronghornStage {

	private final Pipe<PersistedBlobLoadReleaseSchema> fromStoreRelease;
	private final Pipe<PersistedBlobLoadConsumerSchema> fromStoreConsumer;
	private final Pipe<PersistedBlobLoadProducerSchema> fromStoreProducer;
	private final Pipe<PersistedBlobStoreConsumerSchema> toStoreConsumer;
	private final Pipe<PersistedBlobStoreProducerSchema> toStoreProducer;
	
	private byte[] data0;
	private byte[] data1;
	private long[] id;
	private int[] pos;
	private int limit = 0;
	private int activeData=0;
	
	
	public static void newInstance(GraphManager gm, 
			Pipe<PersistedBlobLoadReleaseSchema>  fromStoreRelease,   //ack of release
			Pipe<PersistedBlobLoadConsumerSchema> fromStoreConsumer,  //replay data for watchers
			Pipe<PersistedBlobLoadProducerSchema> fromStoreProducer,  //ack of write
			Pipe<PersistedBlobStoreConsumerSchema> toStoreConsumer,   //command release, replay or clear
			Pipe<PersistedBlobStoreProducerSchema> toStoreProducer    //command store
			) {
		
		new MemorySequentialReplayerStage(gm, 
				fromStoreRelease, fromStoreConsumer, fromStoreProducer,
				toStoreConsumer, toStoreProducer
				);
		
	}

	protected MemorySequentialReplayerStage(GraphManager graphManager, 
			Pipe<PersistedBlobLoadReleaseSchema> fromStoreRelease, 
			Pipe<PersistedBlobLoadConsumerSchema> fromStoreConsumer,
			Pipe<PersistedBlobLoadProducerSchema> fromStoreProducer,
			Pipe<PersistedBlobStoreConsumerSchema> toStoreConsumer,
			Pipe<PersistedBlobStoreProducerSchema> toStoreProducer) {
		super(graphManager, 
				join(fromStoreRelease, fromStoreConsumer, fromStoreProducer), 
				join(toStoreConsumer, toStoreProducer));
		
		this.fromStoreRelease = fromStoreRelease;
		this.fromStoreConsumer = fromStoreConsumer;
		this.fromStoreProducer = fromStoreProducer;
		this.toStoreConsumer = toStoreConsumer;
		this.toStoreProducer = toStoreProducer;
		
	}

	@Override
	public void startup() {
		data0 = new byte[256];
		data1 = new byte[256];
		id = new long[8];
		pos = new int[8*2]; //offset and length
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		while (Pipe.hasContentToRead(toStoreConsumer)) {//command release, replay or clear
			int msgIdx = Pipe.takeMsgIdx(toStoreConsumer);
			
			if (PersistedBlobStoreConsumerSchema.MSG_CLEAR_12==msgIdx) {
				
				//dump all the data
				
				
			} else if (PersistedBlobStoreConsumerSchema.MSG_RELEASE_7==msgIdx) {
				
				//release 
				long id = Pipe.takeLong(toStoreConsumer); //PersistedBlobStoreConsumerSchema.MSG_RELEASE_7_FIELD_BLOCKID_3
				
				
				
				
			} else if (PersistedBlobStoreConsumerSchema.MSG_REQUESTREPLAY_6==msgIdx) {
				
				//replay all
				
				
			} else {
				
				
			}
						
		}
		
		while (Pipe.hasContentToRead(toStoreProducer)) {//command store
			int msgIdx = Pipe.takeMsgIdx(toStoreProducer);
			
			if (PersistedBlobStoreProducerSchema.MSG_BLOCK_1 == msgIdx) {
				
				long blockId = Pipe.takeLong(toStoreProducer);				
				DataInputBlobReader<PersistedBlobStoreProducerSchema> data = Pipe.openInputStream(toStoreProducer);
				
		        //store the data under this block ID.		
				byte[] target = new byte[data.available()];
				data.read(target);
				
				
				
				
				
			} else {
				assert(msgIdx == -1) : "Message type not supported";
				
				//stop??
				
			}			
			
		}
				
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}
	
}
