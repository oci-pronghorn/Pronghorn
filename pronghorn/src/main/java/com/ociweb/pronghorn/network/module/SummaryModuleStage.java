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
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorCollectorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.AppendableByteWriter;

/**
 * _no-docs_
 *
 * @param <T> content type
 * @param <R> revisions
 * @param <V> verbs
 * @param <H> headers
 */
public class SummaryModuleStage<T extends Enum<T> & HTTPContentType,
								R extends Enum<R> & HTTPRevision,
								V extends Enum<V> & HTTPVerb,
								H extends Enum<H> & HTTPHeader> extends AbstractAppendablePayloadResponseStage<T,R,V,H> {

	Logger logger = LoggerFactory.getLogger(DotModuleStage.class);
	
    public static SummaryModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, PipeMonitorCollectorStage monitor, 
    		                      Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs, HTTPSpecification<?, ?, ?, ?> httpSpec) {
    	return new SummaryModuleStage(graphManager, inputs, outputs, httpSpec, monitor);
    }
    
    public static SummaryModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output, HTTPSpecification<?, ?, ?, ?> httpSpec) {
    	PipeMonitorCollectorStage monitor = PipeMonitorCollectorStage.attach(graphManager);		
        return new SummaryModuleStage(graphManager, new Pipe[]{input}, new Pipe[]{output}, httpSpec, monitor);
    }
	
    private final PipeMonitorCollectorStage monitor;
	
	private SummaryModuleStage(GraphManager graphManager, 
			Pipe<HTTPRequestSchema>[] inputs, 
			Pipe<ServerResponseSchema>[] outputs, 
			HTTPSpecification httpSpec, PipeMonitorCollectorStage monitor) {
		super(graphManager, inputs, outputs, httpSpec, estimate(graphManager));
		this.monitor = monitor;
		
		if (inputs.length>1) {
			GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
		}
       
	}
	
	private static int estimate(GraphManager graphManager) {
		return 1000;
		//return (300*GraphManager.countStages(graphManager))+
		//       (400*GraphManager.allPipes(graphManager).length);

	}
	
	@Override
	protected boolean payload(AppendableByteWriter<?> payload, 
			                 GraphManager gm, 
			                 ChannelReader params,
			                 HTTPVerbDefaults verb) {
	
		//logger.info("begin building requested graph");
		//NOTE: this class is exactly the same as DotModuleStage except for this line.
		monitor.writeAsSummary(gm, payload);
		
		//logger.info("finished requested dot");
		return true;
	}
	
	@Override
	public HTTPContentType contentType() {
		return HTTPContentTypeDefaults.DOT;
	}

}
