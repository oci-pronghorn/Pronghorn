package com.ociweb.pronghorn.components.ingestion.csv;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Splits lines in file.
 * TO-DO?
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class LineSplitterFileChannelStage extends LineSplitterByteBufferStage {

	private final FileChannel fileChannel;
	private final boolean showProgress;
	private final Logger log = LoggerFactory.getLogger(LineSplitterFileChannelStage.class);
	private boolean hasRun = false;

	/**
	 *
	 * @param graphManager
	 * @param fileChannel
	 * @param outputRing _out_ The resulting splitted file.
	 */
	public LineSplitterFileChannelStage(GraphManager graphManager, FileChannel fileChannel, Pipe outputRing) {
		super(graphManager, null, outputRing);
		
		this.fileChannel = fileChannel;
		this.showProgress = true;
	}
	
	@Override
	public void run() {
	    if (hasRun) {
	        requestShutdown();
	        return;
	    }
		log.info("running line splitter now");
		try{
			long maxStep = Math.max(1<<27, 1<<outputRing.bitsOfBlogRing);
			final long fileSize = fileChannel.size();
			
			long pos = 0;
			
			int bytesRead;			
			long startPos;
			do {
				startPos = pos;
				resetForNextByteBuffer(this);
				//System.err.println("Reading file bytes "+startPos+" to "+ (startPos+mapSize));
				MappedByteBuffer map = fileChannel.map(FileChannel.MapMode.READ_ONLY, startPos, Math.min(maxStep, fileSize-pos));
				
				do {
					bytesRead = parseSingleByteBuffer(this, map);
				} while (bytesRead<map.limit());
				
				pos += recordStart;					
				if (showProgress) {
					System.out.println(" Progress:"+(startPos+bytesRead)+"/"+fileSize+"     "+ (((float)(startPos+bytesRead)*100f)/(float)fileSize)+"%"  );
				}
			} while (startPos+(long)bytesRead < fileSize);
			shutdownPosition = bytesRead;
			
			requestShutdown();
			log.trace("shutdown the line splitter");
			hasRun = true;
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	
	

}
