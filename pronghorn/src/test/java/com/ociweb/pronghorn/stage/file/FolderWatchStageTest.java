package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.file.schema.FolderWatchSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class FolderWatchStageTest {

	@Test @Ignore
	public void folderWatchTest() {
			
		
		try {
			File startFile = File.createTempFile("watch", "test");
		    
			File folderToWatch = startFile.getParentFile();
			
			
			
			String pathText = folderToWatch.getAbsolutePath();
			
			String[] filePatterns = new String[]{
													"watch%b",
													"foo%bend"
					                               };
						
			StringBuilder[] captured = new StringBuilder[] {
					 new StringBuilder(),
					 new StringBuilder()
			};
			StringBuilder defaultCaptured = new StringBuilder();
			
			
			
			GraphManager gm = new GraphManager();
			Pipe<FolderWatchSchema>[] output = Pipe.buildPipes(filePatterns.length, FolderWatchSchema.instance.newPipeConfig(10,64));
			Pipe<FolderWatchSchema> defaultOutput = FolderWatchSchema.instance.newPipe(4, 64);
			WatchServiceStage.newInstance(gm, pathText, filePatterns, output, defaultOutput, 
					                   WatchServiceStage.EVENT_CREATE|WatchServiceStage.EVENT_DELETE);
			
			ConsoleJSONDumpStage.newInstance(gm, output, captured);
			ConsoleJSONDumpStage.newInstance(gm, defaultOutput, defaultCaptured);
			
			StageScheduler scheduler = StageScheduler.threadPerStage(gm);
			
			
			scheduler.startup();
			
			startFile.delete();
			File newFile = File.createTempFile("foo", "end");
			newFile.deleteOnExit();
		
			try {
				//wait a few ms so the file change will be recognized
				Thread.sleep(20);
			} catch (InterruptedException e) {
			}
						
			//should have deteted this..
			scheduler.shutdown();

			assertTrue(defaultCaptured.toString(), defaultCaptured.length()==0);
			assertTrue(captured[0].toString(), captured[0].toString().startsWith("{\"DeletedFile"));
			assertTrue(captured[1].toString(), captured[1].toString().startsWith("{\"NewFile"));	
			
		
		} catch (IOException e) {
			fail(e.getMessage());
		}
				
	}	
	
}
