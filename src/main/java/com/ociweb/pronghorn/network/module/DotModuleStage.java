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
 * Rest service stage which responds with the dot file needed to display the telemetry.
 * This dot file is a snapshot of the system representing its state within the last 40 ms.
 * @param <T>
 * @param <R>
 * @param <V>
 * @param <H>
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class DotModuleStage<   T extends Enum<T> & HTTPContentType,
								R extends Enum<R> & HTTPRevision,
								V extends Enum<V> & HTTPVerb,
								H extends Enum<H> & HTTPHeader> extends AbstractAppendablePayloadResponseStage<T,R,V,H> {

	private static final Logger logger = LoggerFactory.getLogger(DotModuleStage.class);
	private final String graphName;
	
    public static DotModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, PipeMonitorCollectorStage monitor,
    		                  Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs, HTTPSpecification<?, ?, ?, ?> httpSpec) {

    	return new DotModuleStage(graphManager, inputs, outputs, httpSpec, monitor);
    }
    
    public static DotModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output, HTTPSpecification<?, ?, ?, ?> httpSpec) {
    	PipeMonitorCollectorStage monitor = PipeMonitorCollectorStage.attach(graphManager);		
        return new DotModuleStage(graphManager, new Pipe[]{input}, new Pipe[]{output}, httpSpec, monitor);
    }
	
    private final PipeMonitorCollectorStage monitor;

	/**
	 *
	 * @param graphManager
	 * @param inputs _in_ Pipe containing request for generated .dot file.
	 * @param outputs _out_ Pipe that will contain HTTP response containing newly generated .dot file.
	 * @param httpSpec
	 * @param monitor
	 */
	public DotModuleStage(GraphManager graphManager,
			Pipe<HTTPRequestSchema>[] inputs, 
			Pipe<ServerResponseSchema>[] outputs, 
			HTTPSpecification httpSpec, PipeMonitorCollectorStage monitor) {
		super(graphManager, inputs, outputs, httpSpec, dotEstimate(graphManager));
		this.monitor = monitor;
		this.graphName = "AGraph";

		
		if (inputs.length>1) {
			GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
		}
        GraphManager.addNota(graphManager, GraphManager.SLA_LATENCY, 100_000_000L, this);

	}
	
	private static int dotEstimate(GraphManager graphManager) {
		
		return (300*GraphManager.countStages(graphManager))+
		       (400*GraphManager.allPipes(graphManager).length);

	}

	@Override
	protected boolean payload(AppendableByteWriter<?> payload, 
			                 GraphManager gm, 
			                 ChannelReader params,
			                 HTTPVerbDefaults verb) {
	
		
		//logger.info("begin building requested graph");
		monitor.writeAsDot(gm, graphName, payload);
		
		//logger.info("finished requested dot");
		return true;//return false if we are not able to write it all...
	}
	
	@Override
	public HTTPContentType contentType() {
		return HTTPContentTypeDefaults.DOT;
	}

}
