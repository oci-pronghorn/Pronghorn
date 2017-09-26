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
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ResourceModuleStage<   T extends Enum<T> & HTTPContentType,
									R extends Enum<R> & HTTPRevision,
									V extends Enum<V> & HTTPVerb,
									H extends Enum<H> & HTTPHeader> extends AbstractAppendablePayloadResponseStage<T,R,V,H> {

	private String resource;
	private final byte[] type;
	private static final Logger logger = LoggerFactory.getLogger(ResourceModuleStage.class);

	private final URL resourceURL;
	
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
			
		logger.info("loading resource {} ",resourceName);
		ClassLoader loader = ResourceModuleStage.class.getClassLoader().getSystemClassLoader();
		resourceURL = loader.getResource(resourceName);
		logger.trace("found url {}", resourceURL);
		

		if (null == resourceURL) {
			logger.info("unable to find resource: {} ",resourceName);
			throw new RuntimeException("unable to find resource: "+resourceName);
		} else {
			logger.info("found the resource: {}",resourceName);
		}
				
		this.type = type.getBytes();
		
	}

	@Override
	public void startup() {
		super.startup();
		
		try {

			InputStream stream = resourceURL.openStream();
			
			int x = stream.available();
			//logger.info("file size {}",x);
			
			byte[] bytes = new byte[x];
		
			long startTime = System.currentTimeMillis();
			long timeout = startTime = 10_000;
			int i = 0;
			while(i<x) {
				
				int count = stream.read(bytes, i, x-i);
				assert(count>=0) : "since we know the length should not reach EOF";
		
				i += count;	
				
				if (0 == count) {
					if (System.currentTimeMillis()>timeout) {
						throw new RuntimeException("IO slow reading files from inside the jar.");
					}
				}

			}
			
			resource = new String(bytes);
		
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
