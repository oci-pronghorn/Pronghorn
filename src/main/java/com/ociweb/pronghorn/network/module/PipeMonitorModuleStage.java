package com.ociweb.pronghorn.network.module;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitorToJSON;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 * Rest module which streams live data from specific pipes.
 * This is for deeper telemetry.
 * In progress, not yet enabled.
 * @param <T>
 * @param <R>
 * @param <V>
 * @param <H>
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class PipeMonitorModuleStage<T extends Enum<T> & HTTPContentType,
									R extends Enum<R> & HTTPRevision,
									V extends Enum<V> & HTTPVerb,
									H extends Enum<H> & HTTPHeader> extends PronghornStage {

	private StreamingVisitorReader reader;
	
	private final Pipe<HTTPRequestSchema>[] inputs; 
	private final Pipe<ServerResponseSchema> output; 
	private final Pipe<?> monitorInput;
	private final boolean showBytesAsUTF = false;

	//each request may be a pipe? may have multiple requests for same pipe...

	protected PipeMonitorModuleStage(GraphManager graphManager, 
			Pipe<HTTPRequestSchema>[] inputs, 
			Pipe<?> monitorInput,
			Pipe<ServerResponseSchema> output, 
			HTTPSpecification<T, R, V, H> httpSpec) {
		super(graphManager, inputs, output);
		
		this.inputs = inputs;
		this.monitorInput = monitorInput;
		this.output = output;
		
	}
	
	@Override
	public void startup() {

		//how to write out to multiples???
		
        //create single output upon request..
		//what about multiple users asking for the same data??
		DataOutputBlobWriter out = null; //Data output, HTTP response...
				
		StreamingVisitorReader readerLocal
		              = buildJSONVisitor(out, monitorInput);
		
		reader = readerLocal;
		reader.startup();
	}

	private StreamingVisitorReader buildJSONVisitor(DataOutputBlobWriter out, Pipe<?> input) {
		StreamingVisitorReader readerLocal;
		try{
			
			
            final StreamingReadVisitorToJSON visitor =
            		new StreamingReadVisitorToJSON(out, showBytesAsUTF) {

            	@Override
            	public void visitTemplateOpen(String name, long id) {
            		//out.openField(); //writes to all
            		//any callers need to have the rest wrapped
            		
            		super.visitTemplateOpen(name,id);
            	}
            	
            	@Override
            	public void visitTemplateClose(String name, long id) {
            		super.visitTemplateClose(name, id);
            		
            		//out.closeLowLevelField();
            		
            	}
            	
            	@Override
				public void shutdown() {
					super.shutdown();
					PipeMonitorModuleStage.this.requestShutdown();
				}
			};

			readerLocal = new StreamingVisitorReader(input, visitor );

		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
		return readerLocal;
	}

	@Override
	public void run() {
		reader.run();
	}

	@Override
	public void shutdown() {

		try{
			reader.shutdown();
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

}
