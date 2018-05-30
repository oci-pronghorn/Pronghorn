package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 * Parses OAuth1 tokens and manages its authentication.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class OAuth1AccessTokenResponseStage extends PronghornStage {

	/**
	 *
	 * @param graphManager
	 * @param pipe _in_ The NetResponseSchema containing the OAuth1 tokens.
	 * @param outputPipes _out_ The OAuth1 response.
	 * @param httpSpec
	 */
	public OAuth1AccessTokenResponseStage(GraphManager graphManager, 
			Pipe<NetResponseSchema> pipe,
			Pipe<ServerResponseSchema>[] outputPipes, 
			HTTPSpecification<?, ?, ?, ?> httpSpec) {
		super(graphManager, pipe, outputPipes);
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
