package com.ociweb.pronghorn.stage.test;

import java.io.PrintStream;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitorToJSON;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ConsoleJSONDumpStage<T extends MessageSchema> extends PronghornStage {

	private final Pipe<T> input;

	private StreamingReadVisitor visitor;
	private StreamingVisitorReader reader;
	private PrintStream out = System.out;

	public ConsoleJSONDumpStage(GraphManager graphManager, Pipe<T> input) {
		super(graphManager, input, NONE);
		this.input = input;
	}

	public ConsoleJSONDumpStage(GraphManager graphManager, Pipe<T> input, PrintStream out) {
		this(graphManager, input);
		this.out = out;
	}

	@Override
	public void startup() {

		try{
            visitor = new StreamingReadVisitorToJSON(out) {
				@Override
				public void visitASCII(String name, long id, Appendable value) {
					assert (((CharSequence)value).length()<=input.maxAvgVarLen) : "Text is too long found "+((CharSequence)value).length();
					super.visitASCII(name, id, value);
				}
				@Override
				public void visitUTF8(String name, long id, Appendable value) {
					assert (((CharSequence)value).length()<=input.maxAvgVarLen) : "Text is too long found "+((CharSequence)value).length();
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
