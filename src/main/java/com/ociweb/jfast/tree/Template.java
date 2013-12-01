package com.ociweb.jfast.tree;

import java.util.Iterator;

import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.XMLEvent;

public class Template extends Group {

	//TODO: do not use strings instead use primitve fields to switch templates without GC.
	final String id;
	final String name;
	final String dictionary;
	
	public Template(Iterator<Attribute> attributes) {
		
		String localId = "";
		String localName = "";
		String localDictionary = "";

		while (attributes.hasNext()) {
			Attribute att = attributes.next();
			String name = att.getName().getLocalPart();
						
			if ("id".equals(name)) {
				localId = att.getValue();
			} else if ("name".equals(name)) {
				localName = att.getValue();
			} else if ("dictionary".equals(name)) {
				localDictionary = att.getValue();
			}
		}
		
		this.id = localId;
		this.name = localName;
		this.dictionary = localDictionary;
		
	}

}
