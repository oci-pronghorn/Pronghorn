package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.FolderWatchSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReaderLocal;

public class WatchServiceStage extends PronghornStage {

	private final String pathText;
	private final String[] filePatterns;
	 
	
	private final Pipe<FolderWatchSchema>[] output;
	private final Pipe<FolderWatchSchema> defaultOutput;
	private static final Logger logger = LoggerFactory.getLogger(WatchServiceStage.class);
	
	private FileSystem fileSystem;
	private WatchService watchService;
	private byte[] rootPathTextBytes;
	private TrieParser fileParser;
	
	public static WatchServiceStage newInstance(GraphManager graphManager, 
	            String pathText,
	            String[] filePatterns,
	            Pipe<FolderWatchSchema>[] output,
	            Pipe<FolderWatchSchema> defaultOutput, int kindFlags
			) {
		return new WatchServiceStage(graphManager, pathText, filePatterns, output, defaultOutput, kindFlags);
	}

	public static WatchServiceStage newInstance(GraphManager graphManager, 
            String pathText,
            String[] filePatterns,
            Pipe<FolderWatchSchema>[] output, int kindFlags
		) {
		return new WatchServiceStage(graphManager, pathText, filePatterns, output, kindFlags);
	}
	
	public static final int EVENT_CREATE = 1<<0;
	public static final int EVENT_MODIFY = 1<<1;
	public static final int EVENT_DELETE = 1<<2;
	private int kindFlags;
	
	
	protected WatchServiceStage(GraphManager graphManager, 
			                   String pathText,
			                   String[] filePatterns,
			                   Pipe<FolderWatchSchema>[] output,
			                   Pipe<FolderWatchSchema> defaultOutput, int kindFlags) {
		
		super(graphManager, NONE, null==defaultOutput? output :  join(output,defaultOutput));
		this.pathText = pathText;		
		this.output = output;
		this.defaultOutput = defaultOutput;
		this.filePatterns = filePatterns;
		this.kindFlags = 0!=kindFlags? kindFlags : 7;

	}
	
	protected WatchServiceStage(GraphManager graphManager, 
            String pathText,
            String[] filePatterns,
            Pipe<FolderWatchSchema>[] output, int kindFlags) {
		this(graphManager,pathText,filePatterns,output,null, kindFlags);
	}
	
	@Override
	public void startup() {	
		
		this.fileParser = new TrieParser();
		int i = filePatterns.length;
		while (--i >= 0) {
			this.fileParser.setUTF8Value(filePatterns[i], i);
		};
		
        this.fileSystem = FileSystems.getDefault();        
        Path rootPath = this.fileSystem.getPath(pathText);

		try {
			this.watchService = rootPath.getFileSystem().newWatchService();
			
			List<Kind> kinds = new ArrayList<Kind>();
			kinds.add(StandardWatchEventKinds.OVERFLOW);
			if ( 0 != (kindFlags&EVENT_CREATE)) {
				kinds.add(StandardWatchEventKinds.ENTRY_CREATE);
			}
			if ( 0 != (kindFlags&EVENT_MODIFY)) {
				kinds.add(StandardWatchEventKinds.ENTRY_MODIFY);
			}
			if ( 0 != (kindFlags&EVENT_DELETE)) {
				kinds.add(StandardWatchEventKinds.ENTRY_DELETE);
			}
			rootPath.register(watchService, kinds.toArray(new Kind[kinds.size()]));
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		rootPathTextBytes = rootPath.toAbsolutePath().toString().getBytes();
	}

	private final List<Object> pendingContext = new ArrayList<Object>();
	private final List<Kind>   pendingKind = new ArrayList<Kind>();

		
	@Override
	public void run() {
	
		//confirm that we do the pending work first
		 while (!pendingContext.isEmpty()) {
			  	
			    Path path = (Path)pendingContext.get(0);
			    Path parent = path.getParent();
		    	Path fileName = path.getFileName();
		    	String fileNameText = fileName.toString();			    	
		    	Pipe<FolderWatchSchema> target = lookupTargetPipe(fileNameText);
		    	if (null != target) {
			    	if (Pipe.hasRoomForWrite(target)) {		    		
			    		publish(pendingKind.get(0), parent, fileNameText, target);			    		
			    		pendingContext.remove(0);
			    		pendingKind.remove(0);
			    	} else {
			    		return;//return later when we have room in the pipe
			    	}
		    	}
		  }
		
		//no more pending so process as needed.
		WatchKey key;
		
			while (((key = watchService.poll()) != null) && pendingContext.isEmpty()) {

				//why is this a for loop???
			    for (WatchEvent<?> event : key.pollEvents()) {
			    	final Kind<?> kind = event.kind();
			    	
			    	Path path = (Path)event.context();
			    	
			    	if (null==path) {
			    		continue;
			    	}
			    	
			    	Path parent = path.getParent();
			    	Path fileName = path.getFileName();
			    	String fileNameText = fileName.toString();			    	
			    	Pipe<FolderWatchSchema> target = lookupTargetPipe(fileNameText);
			    	if (null != target) {			    	
				    	if (!Pipe.hasRoomForWrite(target) || !pendingContext.isEmpty()) {
							pendingKind.add(kind);
							pendingContext.add(path);
						} else {
							publish(kind, parent, fileNameText, target);						
						}		    	
			    	}
			    }
			    key.reset();
			}
			
	}

	private void publish(final Kind<?> kind, Path parent, String fileNameText, Pipe<FolderWatchSchema> target) {
				
		if (StandardWatchEventKinds.ENTRY_CREATE == kind) {
			publish(parent, fileNameText, target, Pipe.addMsgIdx(target, FolderWatchSchema.MSG_NEWFILE_1));
		} else if (StandardWatchEventKinds.ENTRY_MODIFY == kind) {
			publish(parent, fileNameText, target, Pipe.addMsgIdx(target, FolderWatchSchema.MSG_UPDATEDFILE_2));
		} else if (StandardWatchEventKinds.ENTRY_DELETE == kind) {	
			publish(parent, fileNameText, target, Pipe.addMsgIdx(target, FolderWatchSchema.MSG_DELETEDFILE_3));
		} else if (StandardWatchEventKinds.OVERFLOW == kind) {	
			//NOTE: some events are lost.... changes happen too quickly
			logger.warn("Some file changes may not have been recognized by this stage, increase RATE or make slower changes to file system.");
		} else {
			throw new UnsupportedOperationException("unknown kind: "+String.valueOf(kind));
		}
	}

	private Pipe<FolderWatchSchema> lookupTargetPipe(String fileNameText) {
		
		final int fileIdx = (int)TrieParserReaderLocal.get().query(fileParser, fileNameText);		
		return fileIdx<0 ? defaultOutput : output[fileIdx];		
	}

	private void publish(Path parent, String fileNameText, Pipe<FolderWatchSchema> target, int size) {
			
		if (null==parent) {
			Pipe.addByteArray(rootPathTextBytes, 0, rootPathTextBytes.length, target);
		} else {
			Pipe.addUTF8(parent.toAbsolutePath().toString(), target);
		}
		
		Pipe.addUTF8(fileNameText, target);
		Pipe.confirmLowLevelWrite(target, size);
		Pipe.publishWrites(target);
	}

}
