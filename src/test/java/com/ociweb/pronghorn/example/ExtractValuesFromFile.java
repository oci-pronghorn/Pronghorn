package com.ociweb.pronghorn.example;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.file.FileBlobReadStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;

public class ExtractValuesFromFile {

	
	public static void main(String[] args) {
		

		String inputPathString = "<some file name>";
		GraphManager gm = new GraphManager();
		
		Pipe<RawDataSchema> output = RawDataSchema.instance.newPipe(10, 2000);				
		new FileBlobReadStage(gm, output, inputPathString);
		
		ParseInput done = new ParseInput(gm, output);
		
		
		StageScheduler s = StageScheduler.defaultScheduler(gm);
		s.startup();
		
		GraphManager.blockUntilStageBeginsShutdown(gm, done);

		
	}

}
