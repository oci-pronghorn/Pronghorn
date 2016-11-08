package com.ociweb.pronghorn.network;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeaderKey;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.pipe.util.hash.PipeHashTable;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.ServiceObjectHolder;
import com.ociweb.pronghorn.util.ServiceObjectValidator;

//Minimal memory usage and leverages SSD.
public class HTTPModuleFileReadStage<   T extends Enum<T> & HTTPContentType,
                                        R extends Enum<R> & HTTPRevision,
                                        V extends Enum<V> & HTTPVerb,
                                        H extends Enum<H> & HTTPHeaderKey> extends AbstractRestStage<T,R,V,H> {

    
    private final static Logger logger = LoggerFactory.getLogger(HTTPModuleFileReadStage.class);
    private final int maxTotalPathLength = 65535;
    private final int maxFileCount = 256;
    
    private final Pipe<HTTPRequestSchema> input;    
    private final Pipe<ServerResponseSchema> output;
    private PipeHashTable outputHash;

    private final FileSystem fileSystem;    
    private FileSystemProvider provider;
    private Set<OpenOption> readOptions;
    
    private TrieParser pathCache;
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
    
    private final String folderRoot;
    private int pathCount;
    private long totalBytes;
    
    
    private Path[] paths;
    private long[] fcId; 
    private long[] fileSizes;
    private byte[][] fileSizeAsBytes;
    private byte[][] etagBytes;
    private int[] type;  //type,  
    
    //move to external utility
    private IntHashTable fileExtensionTable;
    private static final int extHashShift = 3; //note hash map watches first 13 bits only,  4.3 chars 
 
        
    //move to the rest of the context constants
    private static final int OPEN_FILECHANNEL_BITS = 6; //64 open files, no more
    private static final int OPEN_FILECHANNEL_SIZE = 1<<OPEN_FILECHANNEL_BITS;
    private static final int OPEN_FILECHANNEL_MASK = OPEN_FILECHANNEL_SIZE-1;

    private final static int VERB_GET = 0;
    private final static int VERB_HEAD = 1;
        
    public static HTTPModuleFileReadStage<?, ?, ?, ?> newInstance(GraphManager graphManager, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output, HTTPSpecification<?, ?, ?, ?> httpSpec, String rootPath) {
        return new HTTPModuleFileReadStage(graphManager, input, output, httpSpec, rootPath);
    }
    
    public HTTPModuleFileReadStage(GraphManager graphManager, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output, 
                                   HTTPSpecification<T,R,V,H> httpSpec,
                                   String rootPath) {
        
        super(graphManager, input, output, httpSpec);
        this.input = input;
        this.output = output;
        this.fileSystem = FileSystems.getDefault();
        this.folderRoot = rootPath;
                
       // System.out.println("RootFolder: "+folderRoot);
        
        assert( httpSpec.verbMatches(VERB_GET, "GET") );
        assert( httpSpec.verbMatches(VERB_HEAD, "HEAD") );
        
            
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
            result = (result << extHashShift) ^ (0x1F & c); //mask to ignore sign                       
        }        
        return result;
    }
    
    public static int extHash(CharSequence cs) {
        int len = cs.length();        
        int result = cs.charAt(len-1);//init with the last value, will be used twice.    
        while(--len >= 0) {
            result = (result << extHashShift) ^ (0x1F &  cs.charAt(len)); //mask to ignore sign    
        }        
        return result;
    }
    
    private static class FileChannelValidator implements ServiceObjectValidator<FileChannel> {
        
        @Override
        public boolean isValid(FileChannel t) {
            return t.isOpen();
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
    
    private final int hashTableBits = 15; //must hold all the expected files total in bits
    
    @Override
    public void startup() {
        
        this.outputHash = new PipeHashTable(hashTableBits);
        this.fileExtensionTable = buildFileExtHashTable(httpSpec.supportedHTTPContentTypes);

        this.paths = new Path[maxFileCount];
        this.fcId = new long[maxFileCount];
        this.fileSizes = new long[maxFileCount];
        this.fileSizeAsBytes = new byte[maxFileCount][];
        this.etagBytes = new byte[maxFileCount][];
        this.type = new int[maxFileCount];
        //can get a boost from true but how will we know when an upgrade is complete and not get the old pages or bad data??
        //TODO: switch this boolean on the fly when we do releases.
        this.pathCache = new TrieParser(maxTotalPathLength, 2, false, true); //TODO: A, add error support for running out of room in trie
        this.pathCacheReader = new TrieParserReader();
         

        this.channelHolder = new ServiceObjectHolder<FileChannel>(OPEN_FILECHANNEL_BITS, FileChannel.class, new FileChannelValidator() , false);
                
        this.provider = fileSystem.provider();
        this.readOptions = new HashSet<OpenOption>();
        this.readOptions.add(StandardOpenOption.READ);
        
        File rootFileDirectory = new File(folderRoot);
        if (!rootFileDirectory.isDirectory()) {
            throw new UnsupportedOperationException("This must be a folder: "+folderRoot);
        }

        int rootSize = folderRoot.endsWith("/") || folderRoot.endsWith("\\") ? folderRoot.length() : folderRoot.length()+1;
        
        collectAllKnownFiles(rootFileDirectory, rootSize);
        activeFileChannel = null;//NOTE: above method sets activeFileChannel and it must be cleared before run starts.
  
    }


    private void collectAllKnownFiles(File root, int rootSize) {
        File[] children = root.listFiles();
        
        int i = children.length;
      //  System.out.println("collect from "+root+" "+i);
        while (--i>=0) {
            File child = children[i];
            if ((!child.isHidden()) && child.canRead()) {                
                if (child.isDirectory()) {
                    collectAllKnownFiles(child, rootSize);
                } else {
                    setupUnseenFile(pathCache,child.toString(), rootSize);                   
                }       
            }
        }
    }
    
    
    private int setupUnseenFile(TrieParser trie, String pathString, int rootSize) {
        
                int newPathId;
                try {
                    Path path = fileSystem.getPath(pathString);
                    provider.checkAccess(path);
                    newPathId = ++pathCount;
                    byte[] asBytes = pathString.getBytes();
                    
                    logger.debug("FileReadStage is loading {} ",pathString);  
                                        
                    setupUnseenFile(trie, asBytes.length-rootSize, asBytes, rootSize, Integer.MAX_VALUE, newPathId, pathString, path);
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


    @Override
    public void run() {
   
        try {
            
            writeBodiesWhileRoom(activeChannelHigh, activeChannelLow, activeSequenceId, output, activeFileChannel, activePathId);

        } catch (IOException ioex) {
            disconnectDueToError(activeReadMessageSize, output, ioex);
        }
          
   //     System.out.println((null==activeFileChannel) + " &&  "+Pipe.hasContentToRead(input)+ " && "+ Pipe.hasRoomForWrite(output));
        
        while (null==activeFileChannel && Pipe.hasContentToRead(input) && Pipe.hasRoomForWrite(output)) {
            
            int msgIdx = Pipe.takeMsgIdx(input); 
            if (msgIdx == HTTPRequestSchema.MSG_FILEREQUEST_200) {
                activeReadMessageSize = Pipe.sizeOf(input, msgIdx);
                beginReadingNextRequest();                    
            } else {
                if (-1 != msgIdx) {
                    throw new UnsupportedOperationException("Unexpected message "+msgIdx);
                }
                
                Pipe.publishEOF(output);
                requestShutdown(); 
                return; 
            }
        }            
        if (null == activeFileChannel) {
            //only done when nothing is open.
            checkForHotReplace();
        }
    }

    private void checkForHotReplace() {
        //TODO: before return check for file drop of files to be replaced atomically
        
        //TODO: while the change over is in place only use strict checks of the trie.
        
        
    }

    private void beginReadingNextRequest() {
        //channel
        //sequence
        //verb  
        //payload
        //revision
        //context
        
        activeChannelHigh = Pipe.takeInt(input);
        activeChannelLow  = Pipe.takeInt(input); 
   
        activeSequenceId = Pipe.takeInt(input);
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
        int pathId = selectActiveFileChannel(pathCacheReader, pathCache, bytesLength-2, bytesBackingArray, bytesPosition+2, bytesMask);
        int context = Pipe.takeInt(input);
        
        if (pathId<0) {
      	  
        	//send 404
        	//publishError(requestContext, sequence, status, writer, localOutput, channelIdHigh, channelIdLow, httpSpec, revision, contentType);
        	publishErrorHeader(httpRevision, activeRequestContext, activeSequenceId, 404);  
        	
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
	            beginSendingFile(httpRevision, activeRequestContext, pathId, verb, activeSequenceId);
	        } else {
	            publishErrorHeader(httpRevision, activeRequestContext, 0, activeSequenceId, null);
	        }
        }
    }

    private int selectActiveFileChannel(TrieParserReader trieReader, TrieParser trie,
            int bytesLength, final byte[] bytesBackingArray,  int bytesPosition, final int bytesMask) {

        if ('/'==bytesBackingArray[bytesMask&bytesPosition]) {//Always do this?? not sure yet.
            bytesPosition++;
            bytesLength--;
        }     
        
        int pathId = (int)TrieParserReader.query(trieReader, trie, 
                                                         bytesBackingArray, 
                                                         bytesPosition, 
                                                         bytesLength, bytesMask, -1 );      
        
        if (pathId >= 0) {
            if (null!=(activeFileChannel = channelHolder.getValid(fcId[pathId]))) {
            } else {
                System.out.println("not found must lookup again ");
                findAgainFileChannel(pathId);
            }
            return pathId;
        } else {
            //////////////////
            //we have never seen this path before and need to establish the new one
            ////////////////////
            return setupUnseenFile(trie, bytesLength, bytesBackingArray, bytesPosition, bytesMask);
            
        }
        
    }

    private void findAgainFileChannel(int pathId) {
        ///////////////
        //we lost our file channel and need to request a new one.
        //////////////
        try {
        	
        	assert(	paths[pathId].toFile().isFile() );
        	assert(	paths[pathId].toFile().exists() );
        	        			
            activeFileChannel = provider.newFileChannel(paths[pathId], readOptions);
            fcId[pathId] = channelHolder.add(activeFileChannel);
            fileSizes[pathId] = activeFileChannel.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int setupUnseenFile(TrieParser trie, final int bytesLength, final byte[] bytesBackingArray,
                                final int bytesPosition, final int bytesMask) {
        int newPathId;
        try {
            String pathString = Appendables.appendUTF8(new StringBuilder(), bytesBackingArray, bytesPosition, bytesLength, bytesMask).toString();
            Path path = fileSystem.getPath(folderRoot, pathString);          
            
            provider.checkAccess(path);
            newPathId = ++pathCount;
            setupUnseenFile(trie, bytesLength, bytesBackingArray, bytesPosition, bytesMask, newPathId, pathString, path);
            
        } catch (IOException e) {
            activeFileChannel = null;
            newPathId = -1;
        }
        return newPathId;
    }

    private void setupUnseenFile(TrieParser trie, final int bytesLength, final byte[] bytesBackingArray,
            final int bytesPosition, final int bytesMask, int pathId, String pathString, Path path) {
        try {
            //only set this new value if the file exists
            trie.setValue(bytesBackingArray, bytesPosition, bytesLength, bytesMask, pathId);
            
            //NOTE: the type will be 0 zero when not found
            if (pathId>type.length) {
                throw new UnsupportedOperationException("FileReader only supports "+type.length+" files, attempted to add more than this.");
            }
            type[pathId] = IntHashTable.getItem(fileExtensionTable, extHash(bytesBackingArray,bytesPosition, bytesLength, bytesMask));

            StringBuilder builder = new StringBuilder();
            
            activeFileChannel = provider.newFileChannel(paths[pathId] = path, readOptions);
            fcId[pathId] = channelHolder.add(activeFileChannel);
            etagBytes[pathId] = Appendables.appendHexDigits(builder, fcId[pathId]).toString().getBytes();
                        
            fileSizes[pathId] = activeFileChannel.size();   
            builder.setLength(0);
            fileSizeAsBytes[pathId] = Appendables.appendValue(builder, fileSizes[pathId]).toString().getBytes(); //TODO: there is a better way to do this
                    
            
        } catch (IOException e) {
            System.err.println(input);
            logger.error("IO Exception on file {} ",pathString);
            throw new RuntimeException(e);
        }
    }
 
    
    private void beginSendingFile(int httpRevision, int requestContext, int pathId, int verb, int sequence) {
        try {                                               
            //reposition to beginning of the file to be loaded and sent.
            activePayloadSizeRemaining = fileSizes[pathId];
            int status = 200;
                        
            totalBytes += publishHeaderMessage(requestContext, sequence, VERB_GET==verb ? 0 : requestContext, 
            		                           status, output, activeChannelHigh, activeChannelLow,  
                                               httpSpec, httpRevision, type[pathId], fileSizeAsBytes[pathId],  etagBytes[pathId]); 
            
                        
            PipeHashTable.setLowerBounds(outputHash, 1 + Pipe.getBlobWorkingHeadPosition(output) - output.sizeOfBlobRing );
            
            assert(Pipe.getBlobWorkingHeadPosition(output) == positionOfFileDataBegin());
            
                   
            try{              
                publishBodiesMessage(verb, sequence, pathId);
            } catch (IOException ioex) {
                disconnectDueToError(activeReadMessageSize, output, ioex);
            }     
            
        } catch (Exception e) {
            publishErrorHeader(httpRevision, requestContext, pathId, sequence, e);
        }        
    }

	private long positionOfFileDataBegin() {
		return PipeHashTable.getLowerBounds(outputHash)-1+output.sizeOfBlobRing;
	}

    private void publishErrorHeader(int httpRevision, int requestContext, int pathId, int sequence, Exception e) {
        if (null != e) {
            logger.error("Unable to read file for sending.",e);
        }
        //Informational 1XX, Successful 2XX, Redirection 3XX, Client Error 4XX and Server Error 5XX.
        int errorStatus = null==e? 400:500;
        
        publishError(requestContext, sequence, errorStatus, output, activeChannelHigh, activeChannelLow, httpSpec,
                httpRevision, type[pathId]);
        
        Pipe.confirmLowLevelRead(input, activeReadMessageSize);
        Pipe.releaseReadLock(input);
    }

    private void publishErrorHeader(int httpRevision, int requestContext, int sequence, int code) {
        logger.warn("published error "+code);
        publishError(requestContext, sequence, code, output, activeChannelHigh, activeChannelLow, httpSpec, httpRevision, -1);
        
        Pipe.confirmLowLevelRead(input, activeReadMessageSize);
        Pipe.releaseReadLock(input);
    }
    
    private void disconnectDueToError(int releaseSize, Pipe<ServerResponseSchema> localOutput, IOException ioex) {
        logger.error("Unable to complete file transfer to client ",ioex);
                
        //now implement an unexpected disconnect of the connection since we had an IO failure.
        int originalBlobPosition = Pipe.getBlobWorkingHeadPosition(localOutput);
        Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, 0, localOutput);
        Pipe.addIntValue(ServerCoordinator.CLOSE_CONNECTION_MASK | ServerCoordinator.END_RESPONSE_MASK, localOutput);
        Pipe.confirmLowLevelWrite(localOutput, Pipe.sizeOf(localOutput, ServerResponseSchema.MSG_TOCHANNEL_100));
        Pipe.publishWrites(localOutput);
        Pipe.confirmLowLevelRead(input, releaseSize);
        Pipe.releaseReadLock(input);
        
        activeFileChannel = null;
    }
    
    private void publishBodiesMessage(int verb, int sequence, int pathId) throws IOException {
            if (VERB_GET == verb) { //head does not get body

                activePosition = 0;
                activeFileChannel.position(0);
                writeBodiesWhileRoom(activeChannelHigh, activeChannelLow, sequence, output, activeFileChannel, pathId);                             

            } else if (VERB_HEAD == verb){
                activeFileChannel = null;
                Pipe.confirmLowLevelRead(input, activeReadMessageSize);
                totalBytes += Pipe.releaseReadLock(input);
            } else {
                throw new UnsupportedOperationException("Unknown Verb, File reader only supports get and head");
            }
    }

    
    private void writeBodiesWhileRoom(int channelHigh, int channelLow, int sequence, Pipe<ServerResponseSchema> localOutput, FileChannel localFileChannel, int pathId) throws IOException {
       if (null != localFileChannel) {
         long localPos = activePosition;
       //  logger.info("write body {} {}",Pipe.hasRoomForWrite(localOutput), localOutput);
         
         while (Pipe.hasRoomForWrite(localOutput)) {
                           
             // NEW IDEA
             // long copyDistance = Pipe.blobMask(output) & (blobPosition - originalBlobPosition);
             // System.out.println(copyDistance+" vs "+Pipe.blobMask(output)); //NOTE: copy time limits throughput to 40Gbps so larger files can not do 1M msg per second
             // TOOD: BBBB, add messages with gap field to support full zero copy approach to reading the cache.
             //if gap is no larger than 1/2 the max var field length then we could use this technique to avoid the copy below
             //even if this only happens 20% of the time it will stil make a noticable impact in speed.
             /////////
             
            int blobPosition = (int)PipeHashTable.getItem(outputHash, fcId[pathId]);
            
            final int headBlobPosInPipe = Pipe.storeBlobWorkingHeadPosition(localOutput);
            if (true && blobPosition>0 && (fileSizes[pathId]<Pipe.blobMask(localOutput))) { //WARNING: still needs more testing TODO: confirm it works for all layouts.
            	
            	//data is still in the output buffer so copy it from there.
            	int len = Math.min((int)activePayloadSizeRemaining, localOutput.maxAvgVarLen);
                
                Pipe.copyBytesFromToRing(Pipe.blob(localOutput), Pipe.safeBlobPosAdd(blobPosition,localPos), Pipe.blobMask(localOutput), 
                                         Pipe.blob(localOutput), headBlobPosInPipe, Pipe.blobMask(localOutput), len);
                
                publishBodyPart(channelHigh, channelLow, sequence, localOutput, len);   
                localPos += len;
            } else {
            	assert(localFileChannel.isOpen());
            	assert(localFileChannel.position() == localPos) : "independent file position check does not match";
            	//must read from file system
                long len;
                if ((len=localFileChannel.read(Pipe.wrappedWritingBuffers(headBlobPosInPipe, localOutput))) >= 0) {
                    
                	//Not yet complete 
                	logger.info("FileReadStage wrote out {} total file size {} curpos {} ",len,localFileChannel.size(),localFileChannel.position());
                                    	
                	assert(len<Integer.MAX_VALUE);
                    publishBodyPart(channelHigh, channelLow, sequence, localOutput, (int)len);   
                    localPos += len;
                                        
                } else {
                    //logger.info("end of input file detected");
                    
                    Pipe.confirmLowLevelRead(input, activeReadMessageSize);
                    totalBytes += Pipe.releaseReadLock(input);//returns count of bytes used by this fragment                 
                    activeFileChannel = null;
                    //now store the location of this new data.
                    Pipe.unstoreBlobWorkingHeadPosition(localOutput);
                    PipeHashTable.replaceItem(outputHash, fcId[pathId], positionOfFileDataBegin() );
              
                    return;
                }
            }
            
            if (activePayloadSizeRemaining<=0) {
                Pipe.confirmLowLevelRead(input, activeReadMessageSize);
                totalBytes += Pipe.releaseReadLock(input);//returns count of bytes used by this fragment                 
                activeFileChannel = null;
         
                //now store the location of this new data so we can use it as the cache later                
                PipeHashTable.replaceItem(outputHash, fcId[pathId], positionOfFileDataBegin());
                return;
            }
            
            
                
         }
         activePosition = localPos;
       } 
    }


    private void publishBodyPart(int channelHigh, int channelLow, int sequence, Pipe<ServerResponseSchema> localOutput, int len) {

                        
        int payloadMsgSize = Pipe.addMsgIdx(localOutput, ServerResponseSchema.MSG_TOCHANNEL_100); //channel, sequence, context, payload 

        Pipe.addIntValue(channelHigh, localOutput);
        Pipe.addIntValue(channelLow, localOutput);
        
        Pipe.addIntValue(sequence, localOutput);       
        
        int originalBlobPosition = Pipe.unstoreBlobWorkingHeadPosition(localOutput);
        
        //logger.info("publish body part from {} for len of {} ", originalBlobPosition, len);
   
        Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, len, localOutput);

        //NOTE: this field is last so we can return failure and close connection.
        if (  (activePayloadSizeRemaining -= len) > 0) {
            Pipe.addIntValue(0, localOutput); //empty request context, set the full value on the last call.
        } else {
            Pipe.addIntValue(activeRequestContext, localOutput);  
        }
        
        Pipe.confirmLowLevelWrite(localOutput, payloadMsgSize);
        Pipe.publishWrites(localOutput);
        
    }


    @Override
    public void shutdown() {
        System.out.println("total bytes out "+totalBytes);
    }

}
