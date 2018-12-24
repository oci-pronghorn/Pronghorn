package com.ociweb.pronghorn.stage.file;

import java.io.File;
import java.io.IOException;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.PronghornStageProcessor;
import com.ociweb.pronghorn.stage.encrypt.NoiseProducer;
import com.ociweb.pronghorn.stage.encrypt.RawDataCryptAESCBCPKCS5Stage;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageReceiveSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageXmitSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadConsumerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadProducerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadReleaseSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreConsumerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreProducerSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialCtlSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialRespSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FileGraphBuilder {

	public static void buildSequentialReplayer(GraphManager gm,
			Pipe<PersistedBlobLoadReleaseSchema>  fromStoreRelease,   //ack of release
			Pipe<PersistedBlobLoadConsumerSchema> fromStoreConsumer,  //replay data for watchers
			Pipe<PersistedBlobLoadProducerSchema> fromStoreProducer,  //ack of write
			Pipe<PersistedBlobStoreConsumerSchema> toStoreConsumer,   //command release, replay or clear
			Pipe<PersistedBlobStoreProducerSchema> toStoreProducer,   //command store
			short inFlightCount, int largestBlock,
			File targetDirectory, NoiseProducer noiseProducer, 
			PronghornStageProcessor stageProcessor) {
				
		assert(null!=fromStoreRelease);
		assert(null!=fromStoreConsumer);
		assert(null!=fromStoreProducer);
		assert(null!=toStoreConsumer);
		assert(null!=toStoreProducer);
		
		PipeConfig<SequentialCtlSchema> ctlConfig = SequentialCtlSchema.instance.newPipeConfig(inFlightCount);
		PipeConfig<SequentialRespSchema> respConfig = SequentialRespSchema.instance.newPipeConfig(inFlightCount);
		PipeConfig<RawDataSchema> releaseConfig = RawDataSchema.instance.newPipeConfig(inFlightCount, 128);		
		PipeConfig<RawDataSchema> dataConfig = RawDataSchema.instance.newPipeConfig(inFlightCount, largestBlock);
					
				
		Pipe<SequentialCtlSchema>[] control = new Pipe[]  {
				             new Pipe<SequentialCtlSchema>(ctlConfig),
				             new Pipe<SequentialCtlSchema>(ctlConfig),
				             new Pipe<SequentialCtlSchema>(ctlConfig)};
		
		Pipe<SequentialRespSchema>[] response = new Pipe[] {
				             new Pipe<SequentialRespSchema>(respConfig),
				             new Pipe<SequentialRespSchema>(respConfig),
				             new Pipe<SequentialRespSchema>(respConfig)};
		
		Pipe<RawDataSchema>[] fileDataToLoad = new Pipe[] {
							 new Pipe<RawDataSchema>(dataConfig.grow2x()),
							 new Pipe<RawDataSchema>(dataConfig.grow2x()),
							 new Pipe<RawDataSchema>(releaseConfig.grow2x())};
		
		Pipe<RawDataSchema>[] fileDataToSave = new Pipe[] {
							 new Pipe<RawDataSchema>(dataConfig),
							 new Pipe<RawDataSchema>(dataConfig),
				             new Pipe<RawDataSchema>(releaseConfig)};
		
		
		String[] paths = null;
		try {
			paths = new String[]{	
					File.createTempFile("seqRep", ".dat0", targetDirectory).getAbsolutePath(),
					File.createTempFile("seqRep", ".dat1", targetDirectory).getAbsolutePath(),
					File.createTempFile("seqRep", ".idx",  targetDirectory).getAbsolutePath()};
		} catch (IOException e) {
			e.printStackTrace();
		}

		PronghornStage readWriteStage = new SequentialFileReadWriteStage(gm, control, response, 
									     fileDataToSave, fileDataToLoad, 
									     paths);
			
		if (null!=stageProcessor) {
			stageProcessor.process(gm,  readWriteStage);
		}
		if (null != noiseProducer) {
			
			
			Pipe<RawDataSchema>[] cypherDataToLoad = new Pipe[] {
					 new Pipe<RawDataSchema>(dataConfig),
					 new Pipe<RawDataSchema>(dataConfig),
					 new Pipe<RawDataSchema>(releaseConfig)};
	
			Pipe<RawDataSchema>[] cypherDataToSave = new Pipe[] {
					 new Pipe<RawDataSchema>(dataConfig.grow2x()),
					 new Pipe<RawDataSchema>(dataConfig.grow2x()),
		             new Pipe<RawDataSchema>(releaseConfig.grow2x())};
			
			
			int i = 3;
			while (--i>=0) {
				byte[] cypherBlock = noiseProducer.nextCypherBlock();
				
				if (cypherBlock != null) {
					if (cypherBlock.length!=16) {
						throw new UnsupportedOperationException("cypherBlock must be 16 bytes");
					}
				}
				
				Pipe<BlockStorageReceiveSchema> doFinalReceive1 = BlockStorageReceiveSchema.instance.newPipe(7, largestBlock);
				Pipe<BlockStorageXmitSchema> doFinalXmit1 = BlockStorageXmitSchema.instance.newPipe(7, largestBlock);
					
				Pipe<BlockStorageReceiveSchema> doFinalReceive2 = BlockStorageReceiveSchema.instance.newPipe(7, largestBlock);
				Pipe<BlockStorageXmitSchema> doFinalXmit2 = BlockStorageXmitSchema.instance.newPipe(7, largestBlock);
				
				
				
				BlockStorageStage.newInstance(gm, 
									  paths[i]+".tail", 
						              new Pipe[] {doFinalXmit1, doFinalXmit2},
						              new Pipe[] {doFinalReceive1, doFinalReceive2});
				
				
				
				
				RawDataCryptAESCBCPKCS5Stage crypt1 = new RawDataCryptAESCBCPKCS5Stage(gm, 
						cypherBlock, true, cypherDataToSave[i], fileDataToSave[i],
				                         doFinalReceive1, doFinalXmit1);
				
				if (null!=stageProcessor) {
					stageProcessor.process(gm,  crypt1);
				}
				
				RawDataCryptAESCBCPKCS5Stage crypt2 = new RawDataCryptAESCBCPKCS5Stage(gm, 
						cypherBlock, false, fileDataToLoad[i], cypherDataToLoad[i],
				                         doFinalReceive2, doFinalXmit2);
				
				
				if (null!=stageProcessor) {
					stageProcessor.process(gm,  crypt2);
				}
		
			}			
		
			SequentialReplayerStage stage = new SequentialReplayerStage(gm, 
					toStoreConsumer, toStoreProducer, 
					fromStoreRelease, fromStoreConsumer, fromStoreProducer, 
					control, response, cypherDataToSave, cypherDataToLoad, noiseProducer);
			
			
			if (null!=stageProcessor) {
				stageProcessor.process(gm,  stage);
			}
			
		} else {
			SequentialReplayerStage stage = new SequentialReplayerStage(gm, 
					toStoreConsumer, toStoreProducer, 
					fromStoreRelease, fromStoreConsumer, fromStoreProducer, 
					control, response, fileDataToSave, fileDataToLoad, null);
			
			if (null!=stageProcessor) {
				stageProcessor.process(gm,  stage);
			}
			
		}
	}


}
