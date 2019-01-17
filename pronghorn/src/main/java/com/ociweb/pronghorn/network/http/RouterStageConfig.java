package com.ociweb.pronghorn.network.http;

import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public interface RouterStageConfig {

	public int headerId(byte[] bytes);

	public HTTPSpecification httpSpec();
	
	public CompositeRoute registerCompositeRoute(HTTPHeader ... headers);

	public CompositeRoute registerCompositeRoute(JSONExtractorCompleted extractor, HTTPHeader ... headers);

	public byte[] jsonOpenAPIBytes(GraphManager graphManager);
	
}
