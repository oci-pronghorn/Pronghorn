package com.ociweb.pronghorn.network.module;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
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
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class ResourceModuleStage<   T extends Enum<T> & HTTPContentType,
									R extends Enum<R> & HTTPRevision,
									V extends Enum<V> & HTTPVerb,
									H extends Enum<H> & HTTPHeader> extends ByteArrayPayloadResponseStage<T,R,V,H> {

	private static final Logger logger = LoggerFactory.getLogger(ResourceModuleStage.class);
	private final HTTPSpecification httpSpec;
	private StringBuilder temp;
	private TrieParserReader parserReader;
	private TrieParser parser = new TrieParser(100);	
	private int fileCount = 0;
	
	private int activeFileIdx;
	private byte[][] eTag = new byte[0][];
	private byte[][] type = new byte[0][];
	private byte[][] resource = new byte[0][];	
	private URL[] resourceURL = new URL[0];
		
	private final String prefix;
	private String defaultName;
	
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
	
	public ResourceModuleStage(GraphManager graphManager, 
			                   Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs, 
			                   HTTPSpecification httpSpec, String prefix, String resourceName) {
		
		super(graphManager, inputs, outputs, httpSpec);		
		this.httpSpec = httpSpec;
		this.defaultName = resourceName;
		this.prefix = prefix;
		
		if (inputs.length>1) {
			GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
		}
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);

	}

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
		
		if (verb != HTTPVerbDefaults.GET) {
			return null;
		}

		String fileName = defaultName;
		if (params.available()>0) {
			fileName = params.readUTF();
		}
		
		int fileIdx = (int)TrieParserReader.query(parserReader, parser, fileName);

		if (fileIdx<0) {
			
			if (fileName.indexOf("..")>=0) {
				logger.info("unable to find resource: {} ",fileName);
				return null;//can not look this up
			}
			
			URL localURL = ResourceModuleStage.class.getClassLoader().getSystemClassLoader().getResource(prefix+fileName);
			
			if (null == localURL) {
				logger.info("unable to find resource: {} ",fileName);
				return null;
			}

			fileIdx = fileCount++;
			parser.setUTF8Value(fileName, fileIdx);
				
			//grow arrays
			eTag = grow(eTag, fileCount);
			type = grow(type, fileCount);
			resource = grow(resource, fileCount);
			resourceURL = grow(resourceURL, fileCount);
			
			
		    //logger.info("loading resource {} ",resourceName);
			this.resourceURL[fileIdx] = localURL;
			this.type[fileIdx] = HTTPSpecification.lookupContentTypeByExtension(httpSpec, fileName).getBytes();

			
			try {

				InputStream stream = resourceURL[fileIdx].openStream();
				
				final int fileSize = stream.available();			
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
