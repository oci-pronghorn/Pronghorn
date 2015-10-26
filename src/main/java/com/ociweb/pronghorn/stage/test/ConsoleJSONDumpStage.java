package com.ociweb.pronghorn.stage.test;

import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitorToJSON;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ConsoleJSONDumpStage extends PronghornStage {

    private static Logger log = LoggerFactory.getLogger(ConsoleJSONDumpStage.class);
    
	private final Pipe input;
	
	private StreamingReadVisitor visitor;
	private StreamingVisitorReader reader;
	private FieldReferenceOffsetManager from;
	private final int maxString;

	public ConsoleJSONDumpStage(GraphManager graphManager, Pipe input) {
		super(graphManager, input, NONE);
		this.input = input;
		this.maxString = input.maxAvgVarLen;
	}

	@Override
	public void startup() {
		super.startup();		
		PrintStream out = System.out;
		
		try{
			
			from = Pipe.from(input);
            visitor = new StreamingReadVisitorToJSON(out) {
				@Override
				public void visitASCII(String name, long id, Appendable value) {
					assert (((CharSequence)value).length()<maxString) : "Text is too long found "+((CharSequence)value).length();
					super.visitASCII(name, id, value);
				}
				@Override
				public void visitUTF8(String name, long id, Appendable value) {
					assert (((CharSequence)value).length()<maxString) : "Text is too long found "+((CharSequence)value).length();
					super.visitUTF8(name, id, value);
				}
			};
						
			reader = new StreamingVisitorReader(input, visitor );

			reader.startup();
								
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
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
