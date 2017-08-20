package com.ociweb.pronghorn.stage.file;

import java.io.File;
import java.io.IOException;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.encrypt.RawDataCryptAESCBCPKCS5Stage;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileControlSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileResponseSchema;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FileGraphBuilder {

	public static Pipe<PersistedBlobLoadSchema> buildSequentialReplayer(GraphManager gm,
			Pipe<PersistedBlobStoreSchema> toStore, 
			byte multi, byte bits, short inFlightCount, int largestBlock,
			File targetDirectory, 
			byte[] cypherBlock) {
		
		if (cypherBlock != null) {
			if (cypherBlock.length!=16) {
				throw new UnsupportedOperationException("cypherBlock must be 16 bytes");
			}
		}
		
		PipeConfig<SequentialFileControlSchema> ctlConfig = SequentialFileControlSchema.instance.newPipeConfig(inFlightCount);
		PipeConfig<SequentialFileResponseSchema> respConfig = SequentialFileResponseSchema.instance.newPipeConfig(inFlightCount);
		PipeConfig<RawDataSchema> releaseConfig = RawDataSchema.instance.newPipeConfig(inFlightCount, 128);		
		PipeConfig<RawDataSchema> dataConfig = RawDataSchema.instance.newPipeConfig(inFlightCount, largestBlock);
					
		Pipe<PersistedBlobLoadSchema> perLoad = PersistedBlobLoadSchema.instance.newPipe(inFlightCount, largestBlock);
				
		Pipe<SequentialFileControlSchema>[] control = new Pipe[]  {
				             new Pipe<SequentialFileControlSchema>(ctlConfig),
				             new Pipe<SequentialFileControlSchema>(ctlConfig),
				             new Pipe<SequentialFileControlSchema>(ctlConfig)};
		
		Pipe<SequentialFileResponseSchema>[] response = new Pipe[] {
				             new Pipe<SequentialFileResponseSchema>(respConfig),
				             new Pipe<SequentialFileResponseSchema>(respConfig),
				             new Pipe<SequentialFileResponseSchema>(respConfig)};
		
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
		
		new  SequentialFileReadWriteStage(gm, control, response, fileDataToSave, fileDataToLoad, paths); 
		
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
				
				new RawDataCryptAESCBCPKCS5Stage(gm, cypherBlock, true, cypherDataToSave[i], fileDataToSave[i]);
				
				new RawDataCryptAESCBCPKCS5Stage(gm, cypherBlock, false, fileDataToLoad[i], cypherDataToLoad[i]);
			}			
			
			new SequentialReplayerStage(gm, toStore, perLoad, control, response, cypherDataToSave, cypherDataToLoad, multi, bits);
		} else {
			new SequentialReplayerStage(gm, toStore, perLoad, control, response, fileDataToSave, fileDataToLoad, multi, bits);
		}
		return perLoad;
	}

}
