package com.ociweb.pronghorn.components.ingestion.metaMessageUtil;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;


public class MetaMessageUtil  {

    public static FieldReferenceOffsetManager buildFROM(String source) {
    	try {
			return TemplateHandler.loadFrom(source);
		} catch (ParserConfigurationException e) {
			throw new RuntimeException(e);
		} catch (SAXException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }

}
