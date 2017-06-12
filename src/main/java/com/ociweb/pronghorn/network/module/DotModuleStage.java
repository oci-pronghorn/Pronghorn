package com.ociweb.pronghorn.network.module;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class DotModuleStage<   T extends Enum<T> & HTTPContentType,
								R extends Enum<R> & HTTPRevision,
								V extends Enum<V> & HTTPVerb,
								H extends Enum<H> & HTTPHeader> extends AbstractPayloadResponseStage<T,R,V,H> {

    public static DotModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs, HTTPSpecification<?, ?, ?, ?> httpSpec) {
        return new DotModuleStage(graphManager, inputs, outputs, httpSpec);
    }
    
    public static DotModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output, HTTPSpecification<?, ?, ?, ?> httpSpec) {
        return new DotModuleStage(graphManager, new Pipe[]{input}, new Pipe[]{output}, httpSpec);
    }
	
    private final MonitorConsoleStage monitor;
    
	public DotModuleStage(GraphManager graphManager, Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs, HTTPSpecification httpSpec) {
		super(graphManager, inputs, outputs, httpSpec);
		
		monitor = MonitorConsoleStage.attach(graphManager);		
		
	}
	
	protected byte[] buildPayload(Appendable payload, GraphManager gm) {
		byte[] contentType = HTTPContentTypeDefaults.DOT.getBytes();
		
		monitor.writeAsDot(gm, payload);
		
		
		return contentType;
	}

}
