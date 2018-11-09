package com.ociweb.pronghorn.stage.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.spi.FileSystemProvider;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.FolderWatchSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FolderWatchStage extends PronghornStage {

	private final String pathText;
	private final Pipe<FolderWatchSchema>[] outputs;
	
	private FileSystem fileSystem;
	private FileSystemProvider provider;
	private WatchService watchService;
	private Path path;
	
	protected FolderWatchStage(GraphManager graphManager, 
			                   String pathText,			
			                   Pipe<FolderWatchSchema>[] outputs) {
		super(graphManager, NONE, outputs);
		this.pathText = pathText;
		
		this.outputs = outputs;
	}
	
	@Override
	public void startup() {
		
        this.fileSystem = FileSystems.getDefault();
        this.provider = fileSystem.provider();
        
        this.path = this.fileSystem.getPath(pathText);

		try {
			this.watchService = path.getFileSystem().newWatchService();
			path.register(watchService, 
								 StandardWatchEventKinds.ENTRY_CREATE,
					             StandardWatchEventKinds.ENTRY_MODIFY,
					             StandardWatchEventKinds.ENTRY_DELETE            
					);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		
        
        
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

		//TODO: poll this.
		WatchKey watchKey = watchService.poll();
		
		WatchKey key;
		try {
			while ((key = watchService.take()) != null) {
			    for (WatchEvent<?> event : key.pollEvents()) {
			        //process
			    }
			    key.reset();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//need examples of WatchService usage to tie this into the streams
		
		//where does the ingress relete to the JSON parse?
		//      FolderWatch ->  GL RawIngress  ->  behavior
		//      FolderWatch ->  GL JSONIngress  ->  behavior
		//      FolderWatch ->  GL CVSIngress  ->  behavior
		//      FolderWatch ->  GL PropertyIngress  ->  behavior
		
		
		//in HTTP   route --> JSONExtract -> module
		
//      WatchService watcher;
//		Kind<?> kind = null;
//		path.register(watcher, null);
		
	}

}
