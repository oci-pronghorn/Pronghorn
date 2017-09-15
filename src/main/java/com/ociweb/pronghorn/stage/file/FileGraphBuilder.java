package com.ociweb.pronghorn.stage.file;

import java.io.File;
import java.io.IOException;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.encrypt.RawDataCryptAESCBCPKCS5Stage;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageReceiveSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageXmitSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialCtlSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialRespSchema;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FileGraphBuilder {

	public static Pipe<PersistedBlobLoadSchema> buildSequentialReplayer(GraphManager gm,
			Pipe<PersistedBlobStoreSchema> toStore, 
			byte multi, byte bits, short inFlightCount, int largestBlock,
			File targetDirectory, 
			byte[] cypherBlock, long rate) {
		
		if (cypherBlock != null) {
			if (cypherBlock.length!=16) {
				throw new UnsupportedOperationException("cypherBlock must be 16 bytes");
			}
		}
		
		PipeConfig<SequentialCtlSchema> ctlConfig = SequentialCtlSchema.instance.newPipeConfig(inFlightCount);
		PipeConfig<SequentialRespSchema> respConfig = SequentialRespSchema.instance.newPipeConfig(inFlightCount);
		PipeConfig<RawDataSchema> releaseConfig = RawDataSchema.instance.newPipeConfig(inFlightCount, 128);		
		PipeConfig<RawDataSchema> dataConfig = RawDataSchema.instance.newPipeConfig(inFlightCount, largestBlock);
					
		Pipe<PersistedBlobLoadSchema> perLoad = PersistedBlobLoadSchema.instance.newPipe(inFlightCount, largestBlock);
				
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
			paths = new String[]{	File.createTempFile("seqRep", "dat0", targetDirectory).getAbsolutePath(),
					File.createTempFile("seqRep", "dat1", targetDirectory).getAbsolutePath(),
					File.createTempFile("seqRep", "idx", targetDirectory).getAbsolutePath()};
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, new SequentialFileReadWriteStage(gm, control, response, fileDataToSave, fileDataToLoad, paths));
		
		if (null != cypherBlock) {
			
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
				String filePath = "";
				
				Pipe<BlockStorageReceiveSchema> doFinalReceive1 = BlockStorageReceiveSchema.instance.newPipe(10, 1000);
				Pipe<BlockStorageXmitSchema> doFinalXmit1 = BlockStorageXmitSchema.instance.newPipe(10, 1000);
					
				Pipe<BlockStorageReceiveSchema> doFinalReceive2 = BlockStorageReceiveSchema.instance.newPipe(10, 1000);
				Pipe<BlockStorageXmitSchema> doFinalXmit2 = BlockStorageXmitSchema.instance.newPipe(10, 1000);
				
				BlockStorageStage.newInstance(gm, filePath, 
						              new Pipe[] {doFinalXmit1, doFinalXmit2},
						              new Pipe[] {doFinalReceive1, doFinalXmit2});
				
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, new RawDataCryptAESCBCPKCS5Stage(gm, cypherBlock, true, cypherDataToSave[i], fileDataToSave[i],
						                         doFinalReceive1, doFinalXmit1));
				
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, new RawDataCryptAESCBCPKCS5Stage(gm, cypherBlock, false, fileDataToLoad[i], cypherDataToLoad[i],
						                         doFinalReceive2, doFinalXmit2));
			}			
			
			GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, new SequentialReplayerStage(gm, toStore, perLoad, control, response, cypherDataToSave, cypherDataToLoad, multi, bits));
		} else {
			GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, new SequentialReplayerStage(gm, toStore, perLoad, control, response, fileDataToSave, fileDataToLoad, multi, bits));
		}
		return perLoad;
	}

}
