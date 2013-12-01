package com.ociweb.jfast.tree;

import java.util.Iterator;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;

public class TemplateBuilder {

	
	/*
	 * <template dictionary="1" id="1" name="MDIncRefreshSample_1">
    <string id="35" name="MessageType">
        <constant value="X"></constant>
    </string>     <string id="49" name="SenderCompID">
        <constant value="Client"></constant>
    </string>     <string id="56" name="TargetCompID">
        <constant value="Server"></constant>
    </string>     <uint32 id="34" name="MsgSeqNum"></uint32>
    <uint64 id="52" name="SendingTime"></uint64>
    <sequence name="MDEntries">
        <length id="268" name="NoMDEntries"></length>
        <uint32 id="279" name="MDUpdateAction">
            <copy value="1"></copy>
        </uint32>
        <string id="269" name="MDEntryType">
            <copy value="0"></copy>
        </string>
        <uint32 id="278" name="MDEntryID"></uint32>
        <uint32 id="48" name="SecurityID">
            <delta></delta>
        </uint32>
        <decimal id="270" name="MDEntryPx">
            <exponent>
                <default value="-2"></default>
            </exponent>
            <mantissa>
                <delta></delta>
            </mantissa>
        </decimal>
        <int32 id="271" name="MDEntrySize">
            <delta></delta>
        </int32>
        <string id="37" name="OrderID"></string>
        <uint32 id="273" name="MDEntryTime">
            <copy></copy>
        </uint32>
    </sequence>
</template>
	 */
	
	private static final String[] TYPE_NAMES = new String[]{
		                               "uint32","int32","uint64","int64",
			                           "string","decimal","bytevector",
			                           "sequence","group" };
	


	
	//TODO: only run when new template is detected.
	//all binary templates are loaded at startup to be indexed as detected.
	
	public void build(XMLEventReader reader) throws XMLStreamException {
		String elementName = "template";
		Template template = null;
		while (reader.hasNext()) {
			XMLEvent event = reader.nextEvent();
			
			if (event.isStartElement()) {
				StartElement startElement = event.asStartElement();
				String localElementName = startElement.getName().getLocalPart();
				if (elementName.equals(localElementName)) {
					template = new Template(startElement.getAttributes());
					populateGroup(reader, template);
					assert(elementName.equals(event.asEndElement().getName().getLocalPart()));					
				}			
			} else if (event.isEndElement()) {
				break;
			}
		}
		
	}





	private void populateGroup(XMLEventReader reader, Group target) throws XMLStreamException {
		XMLEvent event;
		while (reader.hasNext()) {		
			event = reader.nextEvent();
			//may be end element but only for "template"
			//otherwise this is a child to be added to template
			if (event.isStartElement()) {
				appendChildren(reader, event.asStartElement(), target);
			} else	if (event.isEndElement()) {
				break;
			}
		}
	}




	public void appendChildren(XMLEventReader reader, StartElement startElement, Group group) throws XMLStreamException {

		
		Iterator<Attribute> attributes = startElement.getAttributes();
		
		String fieldId = "";
		String fieldName = "";
        byte isFieldValueOptional = 0; 
        String fieldDictionary = "";// { "template" | "type" | "global" | string }
        String fieldKey = "";
        String fieldNamespace = "";//vendor name space for fieldName
        
		while (attributes.hasNext()) {
			Attribute att = attributes.next();
			String name = att.getName().getLocalPart();
						
			if ("id".equals(name)) {
				fieldId = att.getValue();
			} else if ("name".equals(name)) {
				fieldName = att.getValue();
			} else if ("presence".equals(name)) {
				// "mandatory" is the default value of zero
				if ("optional".equals(att.getValue())) {
					isFieldValueOptional=1;
				}
			} else if ("dictionary".equals(name)) {
				fieldDictionary = att.getValue();
			} else if ("key".equals(name)) {
				fieldKey = att.getValue();
			} else if ("ns".equals(name)) {
				fieldNamespace = att.getValue();
			}
		}
		
		
		switch (indexOf(startElement.getName().getLocalPart(),TYPE_NAMES)) {
			case 0:				//"uint32",
				appendSingleField(reader, group, fieldId, fieldName, fieldDictionary, fieldKey, fieldNamespace, isFieldValueOptional, TypeMask.IntegerUnsigned);
				break;
			case 1:				//"int32",
				appendSingleField(reader, group, fieldId, fieldName, fieldDictionary, fieldKey, fieldNamespace, isFieldValueOptional, TypeMask.IntegerSigned);
				break;
			case 2:  			//"uint64",
				appendSingleField(reader, group, fieldId, fieldName, fieldDictionary, fieldKey, fieldNamespace, isFieldValueOptional, TypeMask.LongUnsigned);
				break;
			case 3:				//"int64",
				appendSingleField(reader, group, fieldId, fieldName, fieldDictionary, fieldKey, fieldNamespace, isFieldValueOptional, TypeMask.LongSigned);
				break;
			case 4:				//"string",
				int operation = 0;
				String value = "";
				int type = TypeMask.TextASCII;
				//TODO:oppContext needed for copy, inc, and delta?
				
				//there is only 1 operation per field
				
				while (reader.hasNext()) {		
					XMLEvent event = reader.nextEvent();
				    //may be end of field or start of operation child element
					if (event.isStartElement()) {
						StartElement oppElement = event.asStartElement();
						operation = indexOf(oppElement.getName().getLocalPart(),OperatorMask.OPP_NAME);
						Iterator<Attribute> attIter = oppElement.getAttributes();
						while (attIter.hasNext()) {
							Attribute att = attIter.next();
							String name = att.getName().getLocalPart();
							if ("value".equals(name)) {
								value = att.getValue();
							} if ("charset".equals(name)) { //"ascii" is the default
								if ("unicode".equals(att.getValue())) {
									type = TypeMask.TextUTF8;									
								}
							}
						}
					} else if (event.isEndElement()) {
						break;
					}
				}
				
				//Can not know type until we read the inner fields
				//then detect is stop element and return 
				if (OperatorMask.Constant==operation) {
					isFieldValueOptional = 0;//constant is never optional
				}
				group.append((type|isFieldValueOptional),operation,value,fieldId,fieldName, fieldKey, fieldDictionary, fieldNamespace);
				break;
			case 5:				//"decimal",
			int operation2 = 0;
			String value2 = "";
			//TODO:oppContext needed for copy, inc, and delta?
			
			//there is only 1 operation per field
			
			while (reader.hasNext()) {		
				XMLEvent event1 = reader.nextEvent();
			    //may be end of field or start of operation child element
				if (event1.isStartElement()) {
					StartElement oppElement1 = event1.asStartElement();
					operation2 = indexOf(oppElement1.getName().getLocalPart(),OperatorMask.OPP_NAME);
					Iterator<Attribute> attIter1 = oppElement1.getAttributes();
					while (attIter1.hasNext()) {
						Attribute att1 = attIter1.next();
						if ("value".equals(att1.getName().getLocalPart())) {
							value2 = att1.getValue();
						}
						
						
					}
					
					//TODO: change the type based on children elements?
					
					//
					
				} else if (event1.isEndElement()) {
					break;
				}
			}
			
			//Can not know type until we read the inner fields
			//then detect is stop element and return 
			if (OperatorMask.Constant==operation2) {
				isFieldValueOptional = 0;//constant is never optional
			}
			group.append((TypeMask.DecimalTwin|isFieldValueOptional),operation2,value2,fieldId,fieldName, fieldKey, fieldDictionary, fieldNamespace); //TODO; this is a problem 
				break;
			case 6:				//"byteVector",
				int operation1 = 0;
				String value1 = "";
				//TODO:oppContext needed for copy, inc, and delta?
				
				//there is only 1 operation per field
				
				while (reader.hasNext()) {		
					XMLEvent event = reader.nextEvent();
				    //may be end of field or start of operation child element
					if (event.isStartElement()) {
						StartElement oppElement = event.asStartElement();
												
						String elementName = oppElement.getName().getLocalPart();
				//		if ("length".equals(elementName)) {
				//		} //TODO: how to do this part?
						
						operation1 = indexOf(elementName, OperatorMask.OPP_NAME);
						Iterator<Attribute> attIter = oppElement.getAttributes();
						while (attIter.hasNext()) {
							Attribute att = attIter.next();
							if ("value".equals(att.getName().getLocalPart())) {
								value1 = att.getValue();
							}
						}
						
						//
						
					} else if (event.isEndElement()) {
						break;
					}
				}
				
				//Can not know type until we read the inner fields
				//then detect is stop element and return 
				if (OperatorMask.Constant==operation1) {
					isFieldValueOptional = 0;//constant is never optional
				}
				group.append((TypeMask.ByteArray|isFieldValueOptional),operation1,value1,fieldId,fieldName, fieldKey, fieldDictionary, fieldNamespace); //TODO: needs length
				break;
			case 7:				//"sequence",
				Group sequence = new Sequence(startElement.getAttributes());
				populateGroup(reader, sequence);
				group.append(sequence);
				break;
			case 8:				//"group"
				Group newGroup = new Group(startElement.getAttributes());
				populateGroup(reader, newGroup);
				group.append(newGroup);
				break;	
		}
	}


	public void appendSingleField(XMLEventReader reader, Group group, String fieldId, String fieldName, String dictionary,
									String key, String namespace, byte isOptional, int baseType) throws XMLStreamException {

		int operation = 0;
		String value = "";
		//TODO:oppContext needed for copy, inc, and delta?
		
		//there is only 1 operation per field
		
		while (reader.hasNext()) {		
			XMLEvent event = reader.nextEvent();
		    //may be end of field or start of operation child element
			if (event.isStartElement()) {
				StartElement oppElement = event.asStartElement();
				operation = indexOf(oppElement.getName().getLocalPart(),OperatorMask.OPP_NAME);
				Iterator<Attribute> attIter = oppElement.getAttributes();
				while (attIter.hasNext()) {
					Attribute att = attIter.next();
					if ("value".equals(att.getName().getLocalPart())) {
						value = att.getValue();
					}
				}
				
				//
				
			} else if (event.isEndElement()) {
				break;
			}
		}
		
		//Can not know type until we read the inner fields
		//then detect is stop element and return 
		if (OperatorMask.Constant==operation) {
			isOptional = 0;//constant is never optional
		}
		group.append((baseType|isOptional),operation,value,fieldId,fieldName, key, dictionary, namespace);
	}


	private int indexOf(String target, String[] values) {
		int i = values.length;
		while (--i>=0 && !target.equals(values[i])) {
			//nothing to do in here.			
		}
		return i;
	}
	

}
