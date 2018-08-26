package com.ociweb.pronghorn.network.module;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.hash.MurmurHash;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.TrieParserReaderLocal;

/**
 * Fetches resources as HTTP responses based on request.
 *
 * @param <T>
 * @param <R>
 * @param <V>
 * @param <H>
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ResourceModuleStage<   T extends Enum<T> & HTTPContentType,
									R extends Enum<R> & HTTPRevision,
									V extends Enum<V> & HTTPVerb,
									H extends Enum<H> & HTTPHeader> extends ByteArrayPayloadResponseStage<T,R,V,H> {

	private static final Logger logger = LoggerFactory.getLogger(ResourceModuleStage.class);
	private final HTTPSpecification httpSpec;
	private StringBuilder temp;
	private TrieParserReader parserReader;
	private TrieParser primaryResourceParser = new TrieParser(100);	
	private int fileCount = 0;
	private final int minVar;
	
	private int activeFileIdx;
	private byte[][] eTag = new byte[0][];
	private byte[][] type = new byte[0][];
	private byte[][] resource = new byte[0][];	
	private URL[] resourceURL = new URL[0];
		
	private final String prefix;
	private String defaultName;
	private byte[] defaultBytes;
	
    public static ResourceModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, 
    		Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs, 
    		HTTPSpecification<?, ?, ?, ?> httpSpec, String prefix, String resourceName) {
        
    	return new ResourceModuleStage(graphManager, inputs, outputs, httpSpec, prefix, resourceName);
        
    }
    
    public static ResourceModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, 
    		Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output, 
    		HTTPSpecification<?, ?, ?, ?> httpSpec, String prefix, String resourceName) {

        return new ResourceModuleStage(graphManager, new Pipe[]{input}, new Pipe[]{output}, httpSpec, prefix, resourceName);
        
    }

	/**
	 *
	 * @param graphManager
	 * @param inputs _in_ Multiple HTTPRequest that are requesting resource(s).
	 * @param outputs _out_ Responds with the resource(s) if it/they exists.
	 * @param httpSpec
	 * @param root
	 * @param resourceName
	 */
	public ResourceModuleStage(GraphManager graphManager, 
			                   Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs, 
			                   HTTPSpecification httpSpec, String root, String resourceName) {
		
		super(graphManager, inputs, outputs, httpSpec);		
		this.httpSpec = httpSpec;
		this.defaultName = resourceName;
		this.defaultBytes = null==resourceName? new byte[0] :resourceName.getBytes();
		
		assert(root.length()>0);
		
		//remove / if present on either end
		String temp = root.endsWith("/") ?  root.substring(0, root.length()-1) :  root;
		this.prefix = temp.startsWith("/") ?  temp.substring(1) : temp;
		
		
		if (inputs.length>1) {
			GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
		}
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);

		this.minVar = minVarLength(outputs);
				
	}
	
	//incoming headers to show that compression is supported.
//	Accept-Encoding: gzip
//	Accept-Encoding: compress
//	Accept-Encoding: deflate
//	Accept-Encoding: br
//	Accept-Encoding: identity
//	Accept-Encoding: *
//
//	// Multiple algorithms, weighted with the quality value syntax:
//	Accept-Encoding: deflate, gzip;q=1.0, *;q=0.5
	
	//These are the headers which need to be sent for commpressed content.
//	header("Pragma: public");
//	header("Expires: 0");
//	header("Cache-Control: must-revalidate, post-check=0, pre-check=0");
//	header("Cache-Control: public");
//	header("Content-Description: File Transfer");
//	header("Content-type: application/octet-stream");
//	header("Content-Disposition: attachment; filename=\"".$filename."\"");
//	header("Content-Transfer-Encoding: binary");
//	header("Content-Length: ".filesize($filepath.$filename));

	@Override
	public void startup() {
		super.startup();
		temp = new StringBuilder();
		parserReader = new TrieParserReader();

	}
	
	@Override
	protected byte[] payload(GraphManager gm, 
			                 DataInputBlobReader<HTTPRequestSchema> params,
			                 HTTPVerbDefaults verb) {
		
		activeFileIdx = -1;//default
		
		if ((verb != HTTPVerbDefaults.GET) && (verb != HTTPVerbDefaults.POST)) {
			logger.warn("unsupported verb {} when requesting a resource, this should be GET", verb);
			definePayload();
			return null;
		}

		String fileName = defaultName;
		
		//set these to default name
		int    fileNamePos = 0;
		int    fileNameLen = defaultBytes.length;
		byte[] fileNameBacking = defaultBytes;
		int    fileNameMask = Integer.MAX_VALUE;
		
		if (params.available()>0) {
			
			//This is always the first and only param therefore we do not need to look it up.
			fileNameLen = params.readShort();	//will be zero length for plain root
			fileNamePos = params.absolutePosition();
			fileNameBacking = DataInputBlobReader.getBackingPipe(params).blobRing;
			fileNameMask = DataInputBlobReader.getBackingPipe(params).blobMask;
			
			if (fileNameLen>1) { //this is 1 so we skip the case of / alone
				fileName = params.readUTFOfLength(fileNameLen);			
			}

			TrieParserReader reader = TrieParserReaderLocal.get();			
			
			StructRegistry structRegistry = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(params));
			
			int structId = DataInputBlobReader.getStructType(params);
		    if (StructRegistry.hasAttachedObject(structRegistry, HTTPHeaderDefaults.ACCEPT_ENCODING, structId)) {
		//    	params.structured().readTextAsParserSource(HTTPHeaderDefaults.ACCEPT_ENCODING, reader)
	//			//TODO: in progress development, adding compressed response support.
//			    int id = 0;
//			    do {		    
//			    	id = (int)reader.parseNext(HTTPUtil.compressionEncodings);
//			    	if (id==HTTPUtil.COMPRESSOIN_GZIP) {
//			    		
//			    		//if we have a file with the .zip ext, then we can choose it...
//			    		//int fileIdx = (int)TrieParserReader.query(parserReader, gzipResourceParser, fileName);
//			    		
//			    		reader.debugAsUTF8(reader, System.out);
//			    		System.out.println();
//			    		
//			    	}		    	
//			    } while (id>=0);
			    
		    
		    }
			
			//logger.info("request for {} len {}",fileName,fileName.length());
		}

		
		int fileIdx = (int)TrieParserReader.query(parserReader, primaryResourceParser, 
				fileNameBacking, fileNamePos,fileNameLen, fileNameMask);

		//logger.info("request for {} fileIdx {} ",fileName,fileIdx);
		
		if (fileIdx<0) {
			
			if (fileName.indexOf("..")>=0) {
				definePayload();
				status = 404;
				logger.warn("unable to support resource: {} ",fileName);
				return null;//can not look this up
			}
			
			String absoluteResourcePath;
			if (fileName.startsWith("/")) { //NOTE: this is only done once per file.
				absoluteResourcePath = prefix+fileName;
			} else {
				absoluteResourcePath = prefix+"/"+fileName;
			}
			
			URL localURL = ResourceModuleStage.class.getClassLoader().getResource(absoluteResourcePath);
			
			if (null == localURL) {
				definePayload();
				status = 404;
				logger.warn("unable to find resource: {} ",absoluteResourcePath);
				return null;
			}

			fileIdx = fileCount++;
			
			primaryResourceParser.setUTF8Value(fileName, fileIdx);
				
			//grow arrays
			eTag = grow(eTag, fileCount);
			type = grow(type, fileCount);
			resource = grow(resource, fileCount);
			resourceURL = grow(resourceURL, fileCount);
			
			
		    //logger.info("loading resource {} ",resourceName);
			this.resourceURL[fileIdx] = localURL;
			this.type[fileIdx] = HTTPSpecification.lookupContentTypeByFullPathExtension(httpSpec, fileName).getBytes();

			try {

				InputStream stream = resourceURL[fileIdx].openStream();
				
				final int fileSize = stream.available();
				if (fileSize > minVar) {
					//TODO: we should slice up the files so this pipe need not be large.
					throw new UnsupportedOperationException("Server maxResponse size is too small. File size is: "+fileSize+" But max server response size is: "+minVar);
				}
				
				resource[fileIdx] = new byte[fileSize];
			
				long startTime = System.currentTimeMillis();
				long timeout = startTime + 10_000;
				int i = 0;
				while(i<fileSize) {
					
					int count = stream.read(resource[fileIdx], i, fileSize-i);
					assert(count>=0) : "since we know the length should not reach EOF";
			
					i += count;	
					
					if (0 == count) {
						if (System.currentTimeMillis()>timeout) {
							throw new RuntimeException("IO slow reading files from inside the jar.");
						}
					}

				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
						
			temp.setLength(0);
			int jenny = MurmurHash.hash32(resource[fileIdx], 0, resource[fileIdx].length, 8675309);
			Appendables.appendHexDigits(temp.append("R-"), jenny).append("-00");
			eTag[fileIdx] = temp.toString().getBytes();
			
		}
		
		activeFileIdx = fileIdx;
				
		//logger.info("request for {} sent {} ",fileName, resource[fileIdx].length);
		definePayload(resource[fileIdx], 0, resource[fileIdx].length, Integer.MAX_VALUE);
		return eTag[fileIdx];
	}
	
	private final URL[] grow(URL[] in, int idx) {
		URL[] result = new URL[idx];
		System.arraycopy(in, 0, result, 0, in.length);
		return result;
	}

	private final byte[][] grow(byte[][] in, int idx) {
		byte[][] result = new byte[idx][];
		System.arraycopy(in, 0, result, 0, in.length);
		return result;
	}

	@Override
	protected byte[] contentType() {
		return activeFileIdx>=0?type[activeFileIdx]:null;
	}
}
