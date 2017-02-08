package com.ociweb.pronghorn.network.module;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.AbstractRestStage;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeaderKey;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.pipe.util.hash.PipeHashTable;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ServiceObjectHolder;
import com.ociweb.pronghorn.util.ServiceObjectValidator;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

//Minimal memory usage and leverages SSD.
public class FileReadModuleStage<   T extends Enum<T> & HTTPContentType,
                                        R extends Enum<R> & HTTPRevision,
                                        V extends Enum<V> & HTTPVerb,
                                        H extends Enum<H> & HTTPHeaderKey> extends AbstractRestStage<T,R,V,H> {

    
    public static class FileReadModuleStageData {
		private Set<OpenOption> readOptions;
		private TrieParser pathCache;
		private Path[] paths;
		private long[] fcId;
		private long[] fileSizes;
		private byte[][] fileSizeAsBytes;
		private byte[][] etagBytes;
		private int[] type;
		public final FileSystem fileSystem = FileSystems.getDefault();
		
		//move to external utility
	    private IntHashTable fileExtensionTable;
	    public static final int extHashShift = 3; //note hash map watches first 13 bits only,  4.3 chars 

		public FileReadModuleStageData(HTTPSpecification httpSpec) {
		  	        
	        fileExtensionTable = buildFileExtHashTable(httpSpec.supportedHTTPContentTypes);
	        
	        int maxFileCount = 128;       
	        setPaths(new Path[maxFileCount]);
	        setFcId(new long[maxFileCount]);
	        setFileSizes(new long[maxFileCount]);
	        setFileSizeAsBytes(new byte[maxFileCount][]);
	        setEtagBytes(new byte[maxFileCount][]);
	        setType(new int[maxFileCount]); 
			
	        
	        setReadOptions(new HashSet<OpenOption>());
	        getReadOptions().add(StandardOpenOption.READ);
		}


		public Set<OpenOption> getReadOptions() {
			return readOptions;
		}

		public void setReadOptions(Set<OpenOption> readOptions) {
			this.readOptions = readOptions;
		}

		public TrieParser getPathCache() {
			return pathCache;
		}

		public void setPathCache(TrieParser pathCache) {
			this.pathCache = pathCache;
		}

		public Path[] getPaths() {
			return paths;
		}

		public void setPaths(Path[] paths) {
			this.paths = paths;
		}

		public long[] getFcId() {
			return fcId;
		}

		public void setFcId(long[] fcId) {
			this.fcId = fcId;
		}

		public long[] getFileSizes() {
			return fileSizes;
		}

		public void setFileSizes(long[] fileSizes) {
			this.fileSizes = fileSizes;
		}

		public byte[][] getFileSizeAsBytes() {
			return fileSizeAsBytes;
		}

		public void setFileSizeAsBytes(byte[][] fileSizeAsBytes) {
			this.fileSizeAsBytes = fileSizeAsBytes;
		}

		public byte[][] getEtagBytes() {
			return etagBytes;
		}

		public void setEtagBytes(byte[][] etagBytes) {
			this.etagBytes = etagBytes;
		}

		public int[] getType() {
			return type;
		}

		public void setType(int[] type) {
			this.type = type;
		}
	}


	private final static Logger logger = LoggerFactory.getLogger(FileReadModuleStage.class);
    
    private final Pipe<HTTPRequestSchema>[] inputs;    
    private final Pipe<ServerResponseSchema>[] outputs;
    private PipeHashTable outputHash;

    private TrieParserReader pathCacheReader;
    
    private ServiceObjectHolder<FileChannel> channelHolder;
    
    private FileChannel activeFileChannel = null;

    private int         activeChannelHigh;
    private int         activeChannelLow;
    
    private long        activePosition;
    private int         activeReadMessageSize;
    private int         activeSequenceId;
    private int         activeRequestContext;
    private int         activePathId;
    private long        activePayloadSizeRemaining;
    private long        activeMessageStart;
    private int         inIdx;
    
    private final int MAX_TEXT_LENGTH = 64;
    private final Pipe<RawDataSchema> digitBuffer = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance,3,MAX_TEXT_LENGTH));
    
    private final String folderRootString;
    private final File   folderRoot;
    private int defaultPathId;
    private String defaultPathFile;
    private byte[] defaultPathBytes;
    
    private int pathCount;

    private long trailingReader;
    private long trailingBlobReader;
    private long totalBytesWritten; 
    
    private FileReadModuleStageData data;
 
	private static final boolean supportInFlightCopy = true;
	private static final boolean supportInFlightCopyByRef = false;  //TODO:still not working,  why do we only find a few of these?
	

    private final int initialMaxTotalPathLength = 1<<14;
	
    //move to the rest of the context constants
    private static final int OPEN_FILECHANNEL_BITS = 6; //64 open files, no more
    private static final int OPEN_FILECHANNEL_SIZE = 1<<OPEN_FILECHANNEL_BITS;
    private static final int OPEN_FILECHANNEL_MASK = OPEN_FILECHANNEL_SIZE-1;

    private final static int VERB_GET = 0;
    private final static int VERB_HEAD = 1;
    
    
    
    //TODO: order supervisor needs more pipes to stop blocks
    //TODO: this class needs to extract the file load path
    
    //TOOD: we need FileChannel objects to be per instance? what about the FC id??
    
    public static FileReadModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] output, HTTPSpecification<?, ?, ?, ?> httpSpec, File rootPath) {
        return new FileReadModuleStage(graphManager, inputs, output, httpSpec, rootPath);
    }
    
    public static FileReadModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output, HTTPSpecification<?, ?, ?, ?> httpSpec, File rootPath) {
        return new FileReadModuleStage(graphManager, new Pipe[]{input}, new Pipe[]{output}, httpSpec, rootPath);
    }
    
    public FileReadModuleStage(GraphManager graphManager, Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs, 
                                   HTTPSpecification<T,R,V,H> httpSpec,
                                   File rootPath) {
        
        super(graphManager, inputs, outputs, httpSpec);
        this.inputs = inputs; //TODO: fix hack must walk all.
        this.outputs = outputs;
        
        this.trailingReader = 0;
        this.trailingBlobReader = 0;
        
        
        this.folderRoot = rootPath.isFile()? rootPath.getParentFile() : rootPath;       
        this.folderRootString = folderRoot.toString();
        
        if (rootPath.isFile()) {
        	defaultPathFile = rootPath.toString();
        }
        
        System.out.println("RootFolder: "+folderRoot);
        
        assert( httpSpec.verbMatches(VERB_GET, "GET") );
        assert( httpSpec.verbMatches(VERB_HEAD, "HEAD") );
        
        this.inIdx = inputs.length;
        
        
            
    }
    //TODO: use PipeHashTable to pull back values that are on the outgoing pipe for use again.
    
    //TODO: parse ahead to determine if we have the same request in a row, then prefix the send with the additional channel IDs
    //      The socket writer will need to pick up this new message to send the same data to multiple callers
    //      Enable some limited out of order processing as long as its on different channels to find more duplicates. 
    
    //TODO: HTTP2, build map of tuples for file IDs to determine the most frequent pairs to be rebuilt as a single file to limit seek time.
    
    //TODO: store the file offsets sent on the pipe, if its still in the pipe, copy to new location rather than use drive.
    
    

    
//  
//  HTTP/1.1 200 OK                                    rarely changes
//  Date: Mon, 23 May 2005 22:38:34 GMT                always changes  
//  Server: Apache/1.3.3.7 (Unix) (Red-Hat/Linux)      never changes
//  Last-Modified: Wed, 08 Jan 2003 23:11:55 GMT       ?? 
//  ETag: "3f80f-1b6-3e1cb03b"                         ??
//  Content-Type: text/html; charset=UTF-8
//  Content-Length: 138
//  Accept-Ranges: bytes
//  Connection: close
//
//  <html>
//  <head>
//    <title>An Example Page</title>
//  </head>
//  <body>
//    Hello World, this is a very simple HTML document.
//  </body>
//  </html>
    

  
    
    public static int extHash(byte[] back, int pos, int len, int mask) {
        int x = pos+len;
        int result = back[mask&(x-1)];
        int c;
        while((--len >= 0) && ('.' != (c = back[--x & mask])) ) {   
            result = (result << FileReadModuleStageData.extHashShift) ^ (0x1F & c); //mask to ignore sign                       
        }        
        return result;
    }
    
    public static int extHash(CharSequence cs) {
        int len = cs.length();        
        int result = cs.charAt(len-1);//init with the last value, will be used twice.    
        while(--len >= 0) {
            result = (result << FileReadModuleStageData.extHashShift) ^ (0x1F &  cs.charAt(len)); //mask to ignore sign    
        }        
        return result;
    }
    
    private static class FileChannelValidator implements ServiceObjectValidator<FileChannel> {
        
        @Override
        public boolean isValid(FileChannel t) {
            return t!=null && t.isOpen();
        }

        @Override
        public void dispose(FileChannel t) {
           try {
               t.close();
            } catch (IOException e) {
                //ignore, we are removing this
            }
        }
    }

    
    @Override
    public void startup() {

    	//local state
        this.pathCacheReader = new TrieParserReader();
        this.channelHolder = new ServiceObjectHolder<FileChannel>(OPEN_FILECHANNEL_BITS, FileChannel.class, new FileChannelValidator() , false);
        
        this.digitBuffer.initBuffers();
        
        //TODO: this full block needs to be shared.
        File rootFileDirectory = folderRoot;

        File[] children = rootFileDirectory.listFiles();
        
        this.outputHash = new PipeHashTable(17);

        
        this.data = new FileReadModuleStageData(httpSpec);
        

        
        //TODO: pull out as common object for all instances
		TrieParser pc = new TrieParser(initialMaxTotalPathLength, 2, false, false);
		
		
		
		
		this.data.setPathCache(pc);
		
		int rootSize = folderRootString.endsWith("/") || folderRootString.endsWith("\\") ? folderRootString.length() : folderRootString.length()+1;
			
		//when reading resources from inside a jar this will be null so we msut load them on the fly, eg when they are asked for.
		if (null!=children) {
			collectAllKnownFiles(rootSize, pc, children);
		}
        
		
		//TODO: GET PREFIX AND STORE THE RESOURCE FOLDER SO WE CAN PULL THE FILES IN AS REQUESTED LATER..
		
        
        
        activeFileChannel = null;//NOTE: above method sets activeFileChannel and it must be cleared before run starts.
  
        //build private ring buffer of outgoing data files
           
        
        int fields = 2; //pos and file id  

    }


    private void collectAllKnownFiles(int rootSize, TrieParser pathCache, File[] children) {
		int i = children.length;
		StringBuilder builder = new StringBuilder();
      //  System.out.println("collect from "+root+" "+i);
        while (--i>=0) {
            File child = children[i];
            if ((!child.isHidden()) && child.canRead()) {                
                if (child.isDirectory()) {
                    collectAllKnownFiles(rootSize, pathCache, child.listFiles());
                } else {
                    setupUnseenFile(pathCache, child.toString(), rootSize, data.fileSystem, builder);                   
                }       
            }
        }
	}
    
    
    private int setupUnseenFile(TrieParser trie, String pathString, final int rootSize, FileSystem fileSystem, StringBuilder builder) {
        
    		//	logger.trace("loading new file: "+pathString);
                int newPathId;
                try {
                    Path path = fileSystem.getPath(pathString);
                    fileSystem.provider().checkAccess(path);
                    newPathId = ++pathCount;
                    byte[] asBytes = pathString.getBytes();
                    
                    if (pathString.equals(defaultPathFile)) {
                    	defaultPathId = newPathId;
                    	defaultPathBytes = Arrays.copyOfRange(asBytes, rootSize, asBytes.length);
                    }
                    
                    //logger.debug("FileReadStage is loading {} ",pathString);  
                                        
                    setupUnseenFile(trie, asBytes.length-rootSize, asBytes, rootSize, Integer.MAX_VALUE, newPathId, pathString, path, builder);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return newPathId;
            }
    

    public static < T extends Enum<T> & HTTPContentType> IntHashTable buildFileExtHashTable(Class<T> supportedHTTPContentTypes) {
        int hashBits = 13; //8K
        IntHashTable localExtTable = new IntHashTable(hashBits);
        
        T[] conentTypes = supportedHTTPContentTypes.getEnumConstants();
        int c = conentTypes.length;
        while (--c >= 0) {            
            if (!conentTypes[c].isAlias()) {//never use an alias for the file Ext lookup.                
                int hash = extHash(conentTypes[c].fileExtension());
                
                if ( IntHashTable.hasItem(localExtTable, hash) ) {                
                    final int ord = IntHashTable.getItem(localExtTable, hash);
                    throw new UnsupportedOperationException("Hash error, check for new values and algo. "+conentTypes[c].fileExtension()+" colides with existing "+conentTypes[ord].fileExtension());                
                } else {
                    IntHashTable.setItem(localExtTable, hash, conentTypes[c].ordinal());
                }
            }
        }
        return localExtTable;
    }

    
    int totalRunCalls = 0;
    int totalFiles = 0;
    
    @Override
    public void run() {
    
    	totalRunCalls++;
    	
    	int iterations = inputs.length;
    	boolean didWork=false;
    	do {    	
    		didWork = false;//be sure we exit if we do no work.
    		
    			if (null==activeFileChannel) {
    				if(--inIdx<0) {
    					inIdx = inputs.length-1;
    				}
    			}
    			
    			Pipe<HTTPRequestSchema> input = inputs[inIdx];
    			Pipe<ServerResponseSchema> output = outputs[inIdx];
    			
		        try {
		            
		            didWork = writeBodiesWhileRoom(activeChannelHigh, activeChannelLow, activeSequenceId, activeFileChannel, activePathId, input, output);
		
		        } catch (IOException ioex) {
		            disconnectDueToError(activeReadMessageSize, ioex, input, output);
		        }
  
		        assert(recordIncomingState(!Pipe.hasContentToRead(input)));
		        assert(recordOutgoingState(!Pipe.hasRoomForWrite(output)));
		        
		        int filesDone = 0;
		        while (null==activeFileChannel && Pipe.hasContentToRead(input) && Pipe.hasRoomForWrite(output)) {
		            filesDone++;
		        	int msgIdx = Pipe.takeMsgIdx(input); 
		            if (msgIdx == HTTPRequestSchema.MSG_FILEREQUEST_200) {
		            	didWork = true;
		            	
		                activeReadMessageSize = Pipe.sizeOf(input, msgIdx);
		                beginReadingNextRequest(input, output);                    
		            } else {
		                if (-1 != msgIdx) {
		                    throw new UnsupportedOperationException("Unexpected message "+msgIdx);
		                }
		                Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
		                Pipe.releaseReadLock(input);
		                
		                Pipe.publishEOF(output);
		                requestShutdown(); 
		                return; 
		            }
		        }       
		        totalFiles+=filesDone;
		        
		        if (null == activeFileChannel) {
		            //only done when nothing is open.
		            checkForHotReplace();
		        } else {
		        	didWork = true; //stay while we have active files open
		        }
		      
    	} while(didWork || --iterations>=0); //we will run iteration loops of did work false.

    }

    
    
    
    private void checkForHotReplace() {
        //TODO: before return check for file drop of files to be replaced atomically
        
        //TODO: while the change over is in place only use strict checks of the trie.
        
        
    }

    private void beginReadingNextRequest(Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output) {
        //channel
        //sequence
        //verb  
        //payload
        //revision
        //context
        
        activeChannelHigh = Pipe.takeInt(input);
        activeChannelLow  = Pipe.takeInt(input); 
           
        activeSequenceId = Pipe.takeInt(input);
        
//        logger.info("file request for channel {} {} seq {} ", activeChannelHigh, activeChannelLow, activeSequenceId);

        int verb = Pipe.takeInt(input);
        
                 
        int meta = Pipe.takeRingByteMetaData(input);
        int bytesLength    = Pipe.takeRingByteLen(input);
        
        assert(0 != bytesLength) : "path must be longer than 0 length";
        if (0 == bytesLength) {
            throw new UnsupportedOperationException("path must be longer than 0 length");
        }
        
        byte[] bytesBackingArray = Pipe.byteBackingArray(meta, input);
        int bytesPosition = Pipe.bytePosition(meta, input, bytesLength);
        int bytesMask = Pipe.blobMask(input);
   
        
        //logger.info("file name: {}", Appendables.appendUTF8(new StringBuilder(), bytesBackingArray, bytesPosition+2, bytesLength-2, bytesMask));
        
        
        ///////////
        //NOTE we have added 2 because that is how it is sent from the routing stage! with a leading short for length
        ////////////
        int httpRevision = Pipe.takeInt(input);
        
        
        
        int pathId = selectActiveFileChannel(pathCacheReader, data.getPathCache(), bytesLength-2, bytesBackingArray, bytesPosition+2, bytesMask);
                
        //Appendables.appendUTF8(System.err, bytesBackingArray, bytesPosition+2, bytesLength-2, bytesMask);
        //System.err.println("new path "+pathId);
        
        int context = Pipe.takeInt(input);
        
        if (pathId<0) {
      	  
        	//send 404
        	//publishError(requestContext, sequence, status, writer, localOutput, channelIdHigh, channelIdLow, httpSpec, revision, contentType);
        	publishErrorHeader(httpRevision, activeRequestContext, activeSequenceId, 404, input, output);  
        	
        //	throw new UnsupportedOperationException("File not found: "+ Appendables.appendUTF8(new StringBuilder(), bytesBackingArray, bytesPosition, bytesLength, bytesMask).toString());
        } else {
	        
	        activePathId = pathId;
	        //This value is ONLY sent on the last message that makes up this response, all others get a zero.
	        activeRequestContext = context | ServerCoordinator.END_RESPONSE_MASK; 
	
	        assert(Pipe.peekInt(input) == bytesLength) : "bytes consumed "+Pipe.peekInt(input)+" must match file path length "+bytesLength+" peek at idx; "+ Pipe.getWorkingTailPosition(input);
	        
	        //////////////////////////
	        //ready to read the file from fileChannel and use type in type[pathId]
	        //////////////////////////
	        if (pathId>=0) {
	            beginSendingFile(httpRevision, activeRequestContext, pathId, verb, activeSequenceId, input, output);
	        } else {
	            publishErrorHeader(httpRevision, activeRequestContext, 0, activeSequenceId, null, output);
	        }
        }
    }

    private int selectActiveFileChannel(TrieParserReader trieReader, TrieParser trie,
            int bytesLength, final byte[] bytesBackingArray,  int bytesPosition, final int bytesMask) {

        if ('/'==bytesBackingArray[bytesMask&bytesPosition]) {//Always do this?? not sure yet.
            bytesPosition++;
            bytesLength--;
        }     
       
        int pathId;
        if (bytesLength>0 || defaultPathId==-1) {
        	pathId = (int)TrieParserReader.query(trieReader, trie, 
                                                         bytesBackingArray, 
                                                         bytesPosition, 
                                                         bytesLength, bytesMask, -1 );    
        } else {       		
       	    pathId = defaultPathId;       		
        }
        
        if (pathId >= 0) {
            if (null!=(activeFileChannel = channelHolder.getValid(data.getFcId()[pathId]))) {
            } else {
                findAgainFileChannel(pathId);
            }
        } else {  
        	
        	
        	
        	
        	
        	logger.info("requested file {} not found", Appendables.appendUTF8(new StringBuilder(), bytesBackingArray, bytesPosition, bytesLength, bytesMask).toString());
        }
        return pathId;
        
    }

    private void findAgainFileChannel(int pathId) {
        ///////////////
        //we lost our file channel and need to request a new one.
        //////////////
        try {
        	
        	assert(	data.getPaths()[pathId].toFile().isFile() );
        	assert(	data.getPaths()[pathId].toFile().exists() );
        	        			
            activeFileChannel = data.fileSystem.provider().newFileChannel(data.getPaths()[pathId], data.getReadOptions());
            data.getFcId()[pathId] = channelHolder.add(activeFileChannel);
            data.getFileSizes()[pathId] = activeFileChannel.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void setupUnseenFile(TrieParser trie, final int bytesLength, final byte[] bytesBackingArray,
                                final int bytesPosition, final int bytesMask, int pathId, String pathString, Path path, StringBuilder builder) {
        try {
            //only set this new value if the file exists
            trie.setValue(bytesBackingArray, bytesPosition, bytesLength, bytesMask, pathId);
            
            //NOTE: the type will be 0 zero when not found
            if (pathId>data.getType().length) {
                throw new UnsupportedOperationException("FileReader only supports "+data.getType().length+" files, attempted to add more than this.");
            }
            data.getType()[pathId] = IntHashTable.getItem(data.fileExtensionTable, extHash(bytesBackingArray,bytesPosition, bytesLength, bytesMask));

            builder.setLength(0);
            
            FileChannel activeFileChannel = data.fileSystem.provider().newFileChannel(data.getPaths()[pathId] = path, data.getReadOptions());
            data.getFcId()[pathId] = channelHolder.add(activeFileChannel);
            data.getEtagBytes()[pathId] = Appendables.appendHexDigits(builder, data.getFcId()[pathId]).toString().getBytes();
                        
            data.getFileSizes()[pathId] = activeFileChannel.size();   
            builder.setLength(0);
            
			data.getFileSizeAsBytes()[pathId] = Appendables.appendValue(builder, data.fileSizes[pathId]).toString().getBytes();  
            
            
        } catch (IOException e) {
            logger.error("IO Exception on file {} ",pathString);
            throw new RuntimeException(e);
        }
    }
 
    
    private void beginSendingFile(int httpRevision, int requestContext, int pathId, int verb, int sequence, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output) {
        try {                                               
            //reposition to beginning of the file to be loaded and sent.
            activePayloadSizeRemaining = data.getFileSizes()[pathId];
            int status = 200;
            boolean reportServer = true;
                        
            byte[] contLoc = null;
            int    contLocLen = 0;            
            if (pathId==defaultPathId) {
            	contLoc = defaultPathBytes;
            	contLocLen = defaultPathBytes.length;
            }
            
          //  logger.info("begin file response for channel {} {}", activeChannelHigh, activeChannelLow);

            //TODO: slow...
            byte[] revision = httpSpec.revisions[httpRevision].getBytes();
            byte[] contentType = httpSpec.contentTypes[data.getType()[pathId]].getBytes();
            
            int bytesConsumed = publishHeaderMessage(requestContext, sequence, VERB_GET==verb ? 0 : requestContext, 
            		                           status, output, activeChannelHigh, activeChannelLow,  
                                               httpSpec, revision, contentType, 
                                               data.getFileSizeAsBytes()[pathId], 0, data.getFileSizeAsBytes()[pathId].length, Integer.MAX_VALUE, 
                                               data.getEtagBytes()[pathId],
                                               reportServer, contLoc, 0,contLocLen,Integer.MAX_VALUE                           
            		); 

            totalBytesWritten = totalBytesWritten + bytesConsumed;
     //       assert(totalBytesWritten>=Pipe.getBlobWorkingHeadPosition(output)) : totalBytesWritten+" must be >= "+Pipe.getBlobWorkingHeadPosition(output);
                        
            //This allows any previous saved values to be automatically removed upon lookup when they are out of range.
            PipeHashTable.setLowerBounds(outputHash, Pipe.getBlobWorkingHeadPosition(output) - output.blobMask );     
            assert(Pipe.getBlobWorkingHeadPosition(output) == positionOfFileDataBegin(output));
                     
            
                   
            try{              
                publishBodiesMessage(this, verb, sequence, pathId, input, output);
            } catch (IOException ioex) {
                disconnectDueToError(activeReadMessageSize, ioex, input, output);
            }     
            
        } catch (Exception e) {
            publishErrorHeader(httpRevision, requestContext, pathId, sequence, e, input, output);
        }        
    }

	private long positionOfFileDataBegin(Pipe<ServerResponseSchema> output) {
		return PipeHashTable.getLowerBounds(outputHash)+output.blobMask;
	}

    private void publishErrorHeader(int httpRevision, int requestContext, int pathId, int sequence, Exception e, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output) {
        if (null != e) {
            logger.error("Unable to read file for sending.",e);
        }
        //Informational 1XX, Successful 2XX, Redirection 3XX, Client Error 4XX and Server Error 5XX.
        int errorStatus = null==e? 400:500;
        
        publishError(requestContext, sequence, errorStatus, output, activeChannelHigh, activeChannelLow, httpSpec,
                httpRevision, data.getType()[pathId]);
        
        Pipe.confirmLowLevelRead(input, activeReadMessageSize);
        Pipe.releaseReadLock(input);
    }

    private void publishErrorHeader(int httpRevision, int requestContext, int sequence, int code, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output) {
        logger.warn("published error "+code);
        publishError(requestContext, sequence, code, output, activeChannelHigh, activeChannelLow, httpSpec, httpRevision, -1);
        
        Pipe.confirmLowLevelRead(input, activeReadMessageSize);
        Pipe.releaseReadLock(input);
    }
    
    private void disconnectDueToError(int releaseSize, IOException ioex, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output) {
        logger.error("Unable to complete file transfer to client ",ioex);
                
        //now implement an unexpected disconnect of the connection since we had an IO failure.
        int originalBlobPosition = Pipe.getBlobWorkingHeadPosition(output);
        Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, 0, output);
        Pipe.addIntValue(ServerCoordinator.CLOSE_CONNECTION_MASK | ServerCoordinator.END_RESPONSE_MASK, output);
        Pipe.confirmLowLevelWrite(output, Pipe.sizeOf(output, ServerResponseSchema.MSG_TOCHANNEL_100));
        Pipe.publishWrites(output);
        Pipe.confirmLowLevelRead(input, releaseSize);
        Pipe.releaseReadLock(input);
        
        activeFileChannel = null;
    }
    
    private static void publishBodiesMessage(FileReadModuleStage that, int verb, int sequence, int pathId, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output) throws IOException {
            if (VERB_GET == verb) { //head does not get body

                that.activePosition = 0;              
                that.writeBodiesWhileRoom(that.activeChannelHigh, that.activeChannelLow, sequence, that.activeFileChannel, pathId, input, output);                             

            } else {
				publishNoBodyMessage(that, verb, input);
            }
    }

	private static void publishNoBodyMessage(FileReadModuleStage that, int verb, Pipe<HTTPRequestSchema> input) {
		if (VERB_HEAD == verb){
			
		    that.activeFileChannel = null;
		    Pipe.confirmLowLevelRead(input, that.activeReadMessageSize);
		    Pipe.releaseReadLock(input);
		    
		} else {
			
		    otherVerbs();
		    
		}
	}

	private static void otherVerbs() {
		throw new UnsupportedOperationException("Unknown Verb, File reader only supports get and head");
	}

    int inFlightRef  = 0;
    int inFlightCopy = 0;    
    int fromDisk = 0;
    
    long lastTime = 0;
    
    
    private boolean writeBodiesWhileRoom(int channelHigh, int channelLow, int sequence, FileChannel localFileChannel, int pathId,
    		                             Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output) throws IOException {

    	boolean didWork = false;
    	
       if (null != localFileChannel) {
         long localPos = activePosition;
       //  logger.info("write body {} {}",Pipe.hasRoomForWrite(localOutput), localOutput);
         
         
         debugFileProgress();
         
         
         while (Pipe.hasRoomForWrite(output, Pipe.sizeOf(ServerResponseSchema.instance, ServerResponseSchema.MSG_SKIP_300)+Pipe.sizeOf(ServerResponseSchema.instance, ServerResponseSchema.MSG_TOCHANNEL_100)  )) {
                           
        	 didWork = true;
        	 
             final long fcId = data.getFcId()[pathId];
             
             final int oldBlobPosition = (int)PipeHashTable.getItem(outputHash, fcId);
            
            int headBlobPosInPipe = Pipe.storeBlobWorkingHeadPosition(output);
            int blobMask = Pipe.blobMask(output);
			final long fileSize = data.getFileSizes()[pathId];
			if (supportInFlightCopy && 
					/*Only use if the file was written previously*/ oldBlobPosition>0 && 
					/*Only use if the full file can be found */fileSize<blobMask) { 
            
            	//do not copy more than 1 fragment at this time
            	int len = Math.min((int)activePayloadSizeRemaining, output.maxAvgVarLen);
            	int prevBlobPos = Pipe.safeBlobPosAdd(oldBlobPosition,localPos);
            	final byte[] blob = Pipe.blob(output);				                
            	
            	
            	
            	/////////////////////////////////////////////
            	//choose between zero copy re-use and arrayCopy re-use
            	////////////////////////////////////////////
                boolean foundFile = false;
                int countOfBytesToSkip = 0;
                
                if (supportInFlightCopyByRef) {
	            	
	            	final int blobWorkingHeadPosition = Pipe.getBlobWorkingHeadPosition(output);
	            	assert(totalBytesWritten>=blobWorkingHeadPosition) : totalBytesWritten+" must be >= "+blobWorkingHeadPosition;
					final int maskedBlobWorkingHeadPosition = blobWorkingHeadPosition&Pipe.blobMask(output);;
	            	
					final int blobSize = output.sizeOfBlobRing;
					final int slabMask = Pipe.slabMask(output);
	            	final long workingHead = Pipe.workingHeadPosition(output);
	            	int[] slab = Pipe.slab(output);
	            	
	            	final long bytesWrittenLimmit = (totalBytesWritten-((long) blobSize ));
	            	
	            	long checkPos = moveAhead(slabMask, workingHead, slab, bytesWrittenLimmit);
	            	
	            	trailingReader = checkPos;
	                long checkBob = trailingBlobReader;
	                long limit = checkBob + (2 * output.maxAvgVarLen);
                                
	               // System.out.println(output.maxAvgVarLen);
	                
//                System.err.println("DDDD blob "+trailingBlobReader+" < "+totalBytesWritten+"-"+ localOutput.sizeOfBlobRing+"      "+trailingReader+"<"+workingHead+
//                		          "   trailing write "+(Pipe.blobMask(localOutput)&trailingBlobReader)+" vs "+(Pipe.blobMask(localOutput)&blobWorkingHeadPosition));
//               

	                while (checkPos<=workingHead /*&& checkBob<limit*/) {
	                	
	                	
	                	final int msgIdx = slab[Pipe.slabMask(output)&(int)checkPos];
	                	assert(msgIdx == ServerResponseSchema.MSG_TOCHANNEL_100 ||
	                		   msgIdx == ServerResponseSchema.MSG_SKIP_300) : "read id was 0x"+Integer.toHexString(msgIdx);
	                	
	                	final int size = Pipe.sizeOf(ServerResponseSchema.instance, msgIdx);                	
	                	
	            		int bytesConsumed = slab[Pipe.slabMask(output)&(int)(size+checkPos-1)]; 
	            		
	            		int b = (int)checkBob&Pipe.blobMask(output);
	            		if (b<maskedBlobWorkingHeadPosition) {
	            			b+=output.sizeOfBlobRing;
	            		}
	
	            		int skipSize = b - maskedBlobWorkingHeadPosition; 
	            	      		            		
	            		
	            		int peekFcIdx = (int)(checkBob+bytesConsumed-8)&Pipe.blobMask(output);
	            		
	            		///////////////////////// TODO: add these ideas....
	            		//if we find an equal64 no match followed by a match it is either
	            		// 1. the full message right after the header
	            		// 2. the first chunk of the messge right after the header
	            		/////////////////////////
	            		
	            		
	            		if ((bytesConsumed == (8+fileSize))
	            			//&& checkBob>=blobWorkingHeadPosition	
	            			&& msgIdx == ServerResponseSchema.MSG_TOCHANNEL_100
	            			&& equal64(blob, Pipe.blobMask(output), peekFcIdx, fcId) 
	            			
	            				) {
	
	            			foundFile = true;
	            			
	            			countOfBytesToSkip = skipSize;
	            			
	            			break;//found one do not continue;
	            		} 
	            		
	            		checkPos+=size;
	            		checkBob+=bytesConsumed;                	
	                	
	                }
	                
                }

            	
            	useInFlightData(output, headBlobPosInPipe, blobMask, len, prevBlobPos, blob, foundFile,	countOfBytesToSkip);
    
                activeMessageStart = publishBodyPart(channelHigh, channelLow, sequence, output, len, data.getFcId()[pathId]);   
                
               // System.err.println("coped data body from "+(output.blobMask&prevBlobPos)+" to "+(output.blobMask&headBlobPosInPipe)+" remaining "+activePayloadSizeRemaining);
                
				localPos += len;
            } else {
            	if (activePosition==0) {
            		activeFileChannel.position(0); //NOTE: we are careful to only do this when we are reading from disk.
            	}
            	fromDisk++;
            	//logger.info("copied the file from disk {} times",fromDisk);
            	assert(localFileChannel.isOpen());
            	assert(localFileChannel.position() == localPos) : "independent file position check does not match";
            	
            	//must read from file system
                long len;
                if ((len=localFileChannel.read(Pipe.wrappedWritingBuffers(headBlobPosInPipe, output))) >= 0) {
                    
                	//logger.info("FileReadStage wrote out {} total file size {} curpos {} ",len,localFileChannel.size(),localFileChannel.position());
                                    	
                	assert(len<Integer.MAX_VALUE);
                	activeMessageStart = publishBodyPart(channelHigh, channelLow, sequence, output, (int)len, data.getFcId()[pathId]);    
                    
                	localPos += len;
                                        
                } else {
                	//len is < 0 marking the end of the file. We have nothing to publish.
                    //logger.info("end of input file detected");
                                    	
        			////////////////////////////////////
        			//finish the end of file send
        			//////////////////////////////////
                    Pipe.confirmLowLevelRead(input, activeReadMessageSize);
                                        
                    Pipe.releaseReadLock(input);//returns count of bytes used by this fragment                 
                                                          
                    activeFileChannel = null;
                    //now store the location of this new data.
                    Pipe.unstoreBlobWorkingHeadPosition(output);
                                        
                    //this is a file write because the data is no where to be found on the pipe previously
                    assert(activeMessageStart>=0);
                   
                    long dataPos = positionOfFileDataBegin(output);
                    PipeHashTable.replaceItem(outputHash, data.getFcId()[pathId], dataPos);

                    //clear value
                    activeMessageStart=-1;
                    
                    return didWork;
                }
            }
            
			////////////////////////////////////
			//finish the end of file send
			//////////////////////////////////
            if (activePayloadSizeRemaining<=0) {
            	
                Pipe.confirmLowLevelRead(input, activeReadMessageSize);
                                
                Pipe.releaseReadLock(input);//returns count of bytes used by this fragment    
                                
                activeFileChannel = null;
         
                //now store the location of this new data so we can use it as the cache later   
                assert(activeMessageStart>=0);
                
                long dataPos = positionOfFileDataBegin(output);
                
                PipeHashTable.replaceItem(outputHash, data.getFcId()[pathId], dataPos);

                //clear value
                activeMessageStart=-1;
                
                return didWork;
            } else {            	
            	logger.trace("remaining bytes for this file {}, will write the rest on next pass",activePayloadSizeRemaining); //positive bytes required            	
            }           
            
                
         }
         activePosition = localPos;
       } 
       return didWork;
    }

	private void useInFlightData(Pipe<ServerResponseSchema> output, int headBlobPosInPipe, int blobMask, int len,
			int prevBlobPos, final byte[] blob, boolean foundFile, int countOfBytesToSkip) {
		
		if ((!supportInFlightCopyByRef) || (!foundFile)) {           	
			
			inFlightCopy++;
			Pipe.copyBytesFromToRing(blob, prevBlobPos, blobMask, blob, headBlobPosInPipe, blobMask, len);  


		} else {
			
			useInFlightRefData(output, countOfBytesToSkip);
		}
		
	}

	private void useInFlightRefData(Pipe<ServerResponseSchema> output, int countOfBytesToSkip) {
		int headBlobPosInPipe;
		inFlightRef++;
		
		//   		System.err.println("found one more in flight copy"+inFlightRef);
		
		int size = Pipe.addMsgIdx(output, ServerResponseSchema.MSG_SKIP_300);
		
		Pipe.addAndGetBytesWorkingHeadPosition(output, countOfBytesToSkip);//he head is moved becauase we want to skip these bytes.
		
		Pipe.addBytePosAndLen(output, Pipe.unstoreBlobWorkingHeadPosition(output), countOfBytesToSkip);
		Pipe.confirmLowLevelWrite(output, size);
		int consumed = Pipe.publishWrites(output);
		assert(countOfBytesToSkip==consumed); 
		
		totalBytesWritten = totalBytesWritten+countOfBytesToSkip;//for accounting we must add this just as though we wrote it.
		
		headBlobPosInPipe = Pipe.storeBlobWorkingHeadPosition(output);
	}

	private void debugFileProgress() {
		boolean debug = false;
         if (debug) {
	         long now = System.currentTimeMillis();
	         if (now>lastTime) {	        	 
	        		logger.info("total bytes out {} inFlightRef {} inFlightCopy {} fromDisk {} ",totalBytesWritten, inFlightRef, inFlightCopy, fromDisk);	        	 
	        	 lastTime = now+2_000;
	         }
         }
	}

	private long moveAhead(final int slabMask, final long workingHead, int[] slab, final long bytesWrittenLimmit) {
		long checkPos = trailingReader;
		while ((checkPos < workingHead) //do not get ahead of where we are writing.            			
				&& (
						(trailingBlobReader < bytesWrittenLimmit)            			   
				   )            			
				) {
			
			checkPos += Pipe.sizeOf(ServerResponseSchema.instance, slab[slabMask&(int)checkPos]);
			trailingBlobReader = trailingBlobReader+slab[slabMask&(int)(checkPos-1)];
			
		}
		//now look ahead to find a potential match
		return checkPos;
	}

	/*
	 * 
	 * previousPos holds the last known time that this exact same payload was sent, this allows consumers to avoid a deep equals check.
	 */
    private long publishBodyPart(int channelHigh, int channelLow, int sequence, Pipe<ServerResponseSchema> output, int len, long fcId) {

        
    	final long messageStartPosition = Pipe.workingHeadPosition(output);
    	final int initBlob = Pipe.getBlobWorkingHeadPosition(output);
    	
        int payloadMsgSize = Pipe.addMsgIdx(output, ServerResponseSchema.MSG_TOCHANNEL_100); //channel, sequence, context, payload 

        Pipe.addIntValue(channelHigh, output);
        Pipe.addIntValue(channelLow, output);
        
        Pipe.addIntValue(sequence, output);       
                
   
        Pipe.moveBlobPointerAndRecordPosAndLength(Pipe.unstoreBlobWorkingHeadPosition(output), len, output);

        //NOTE: this field is last so we can return failure and close connection.
        if (  (activePayloadSizeRemaining -= len) > 0) {
            Pipe.addIntValue(0, output); //empty request context, set the full value on the last call.
        } else {
            Pipe.addIntValue(activeRequestContext, output);  
        }
        
        Pipe.confirmLowLevelWrite(output, payloadMsgSize);
        
        if (supportInFlightCopyByRef) {
	        //we can hide 8 bytes here between the data files.
	        write64(Pipe.blob(output),Pipe.blobMask(output), Pipe.getBlobWorkingHeadPosition(output), fcId);
	        
	        totalBytesWritten = totalBytesWritten + Pipe.publishWrites(output,8);
	        assert(totalBytesWritten>=Pipe.getBlobWorkingHeadPosition(output)) : totalBytesWritten+" must be >= "+Pipe.getBlobWorkingHeadPosition(output);        
	        
	        assert (equal64(Pipe.blob(output),Pipe.blobMask(output), initBlob+Pipe.slab(output)[Pipe.slabMask(output)&((int)messageStartPosition+payloadMsgSize-1)]-8, fcId));
        } else {
        	 totalBytesWritten = totalBytesWritten + Pipe.publishWrites(output);
        	
        }
        
        return messageStartPosition;
    }

    private static void write64(byte[] buf, int mask, int pos, long v) {
    	    	
        buf[mask & pos++] = (byte)(v >>> 56);
        buf[mask & pos++] = (byte)(v >>> 48);
        buf[mask & pos++] = (byte)(v >>> 40);
        buf[mask & pos++] = (byte)(v >>> 32);
        buf[mask & pos++] = (byte)(v >>> 24);
        buf[mask & pos++] = (byte)(v >>> 16);
        buf[mask & pos++] = (byte)(v >>> 8);
        buf[mask & pos++] = (byte) v;
    }
    
    private static boolean equal64(byte[] buf, int mask, int pos, long v) {

        return
    	(buf[mask & pos++] == (byte)(v >>> 56)) &&
        (buf[mask & pos++] == (byte)(v >>> 48)) &&
        (buf[mask & pos++] == (byte)(v >>> 40)) &&
        (buf[mask & pos++] == (byte)(v >>> 32)) &&
        (buf[mask & pos++] == (byte)(v >>> 24)) &&
        (buf[mask & pos++] == (byte)(v >>> 16)) &&
        (buf[mask & pos++] == (byte)(v >>> 8)) &&
        (buf[mask & pos++] == (byte) v);
    }
    

    @Override
    public void shutdown() {
    	assert(reportRecordedStates(getClass().getSimpleName()));
    	logger.info("total calls to run: {} avgFilesPerRun: {}",totalRunCalls,(totalFiles/totalRunCalls));
    	logger.info("total bytes out {} inFlightRef {} inFlightCopy {} fromDisk {} ",totalBytesWritten, inFlightRef, inFlightCopy, fromDisk);
    }

}
