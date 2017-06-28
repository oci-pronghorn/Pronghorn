package com.ociweb.pronghorn.network.module;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ResourceModuleStage<   T extends Enum<T> & HTTPContentType,
									R extends Enum<R> & HTTPRevision,
									V extends Enum<V> & HTTPVerb,
									H extends Enum<H> & HTTPHeader> extends AbstractAppendablePayloadResponseStage<T,R,V,H> {

	private final String resource;
	private final byte[] type;
	private static final Logger logger = LoggerFactory.getLogger(ResourceModuleStage.class);
		
    public static ResourceModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, 
    		Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs, 
    		HTTPSpecification<?, ?, ?, ?> httpSpec, String resourceName, HTTPContentType type) {
        
    	return new ResourceModuleStage(graphManager, inputs, outputs, httpSpec, resourceName, type);
        
    }
    
    public static ResourceModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, 
    		Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output, 
    		HTTPSpecification<?, ?, ?, ?> httpSpec, String resourceName, HTTPContentType type) {
    	
        return new ResourceModuleStage(graphManager, new Pipe[]{input}, new Pipe[]{output}, httpSpec, resourceName, type);
        
    }
	
	public ResourceModuleStage(GraphManager graphManager, 
			                   Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs, 
			                   HTTPSpecification httpSpec, String resourceName, HTTPContentType type) {
		
		super(graphManager, inputs, outputs, httpSpec);
		
		try {
			
			ClassLoader loader = getClass().getClassLoader().getSystemClassLoader();
			
			
			InputStream stream = loader.getResourceAsStream(resourceName);
			
			if (null == stream) {
				logger.info("unable to find resource: "+resourceName);
			}
			
			int x = stream.available();
			
			//logger.info("file size {}",x);
			
			byte[] bytes = new byte[x];
		
			for(int i = 0; i<x; i++) {
				int r = stream.read();
				assert(r != -1);
				bytes[i] = (byte)r; 
			}
			
			resource = new String(bytes);
			
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
				
		this.type = type.getBytes();
		
	}


	@Override
	protected byte[] buildPayload(Appendable payload, GraphManager gm, 
			                      DataInputBlobReader<HTTPRequestSchema> params,
			                      HTTPVerbDefaults verb) {
		
		if (verb != HTTPVerbDefaults.GET) {
			return null;
		}
		
		try {
			payload.append(resource);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		return type;
	}

}
