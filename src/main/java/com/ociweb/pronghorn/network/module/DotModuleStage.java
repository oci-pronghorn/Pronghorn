package com.ociweb.pronghorn.network.module;

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
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class DotModuleStage<   T extends Enum<T> & HTTPContentType,
								R extends Enum<R> & HTTPRevision,
								V extends Enum<V> & HTTPVerb,
								H extends Enum<H> & HTTPHeader> extends AbstractAppendablePayloadResponseStage<T,R,V,H> {

	Logger logger = LoggerFactory.getLogger(DotModuleStage.class);
	
    public static DotModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs, HTTPSpecification<?, ?, ?, ?> httpSpec) {
    	MonitorConsoleStage monitor = MonitorConsoleStage.attach(graphManager);	
    	return new DotModuleStage(graphManager, inputs, outputs, httpSpec, monitor);
    }
    
    public static DotModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output, HTTPSpecification<?, ?, ?, ?> httpSpec) {
    	MonitorConsoleStage monitor = MonitorConsoleStage.attach(graphManager);		
        return new DotModuleStage(graphManager, new Pipe[]{input}, new Pipe[]{output}, httpSpec, monitor);
    }
	
    private final MonitorConsoleStage monitor;
    
	private DotModuleStage(GraphManager graphManager, 
			Pipe<HTTPRequestSchema>[] inputs, 
			Pipe<ServerResponseSchema>[] outputs, 
			HTTPSpecification httpSpec, MonitorConsoleStage monitor) {
		super(graphManager, inputs, outputs, httpSpec);
		this.monitor = monitor;
		
	}

	@Override
	protected byte[] buildPayload(Appendable payload, GraphManager gm, DataInputBlobReader<HTTPRequestSchema> params,
			HTTPVerbDefaults verb) {
		
		if (verb != HTTPVerbDefaults.GET) {
			return null;
		}		
		//logger.info("begin building requested graph");
		monitor.writeAsDot(gm, payload);		
		//logger.info("finished requested dot");
		return HTTPContentTypeDefaults.DOT.getBytes();
	}

}
