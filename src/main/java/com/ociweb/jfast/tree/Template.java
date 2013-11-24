package com.ociweb.jfast.tree;

import com.ociweb.jfast.Operator;
import com.ociweb.jfast.field.TypeMask;

public class Template {

	private final String templateName; 
	private final int id;
	
	//dictionary { "template" | "type" | "global" | string }
    //lookup the right dictionary and set it here instead of the string.	
	private String dictionaryAttr;
	//TODO: xml parse will need to convert strings into ids for use if possible
	
	private String typeRef;
	
	private GroupBuilder builder;

	public Template(String templateNsName,int id) {
		this.templateName = templateNsName;
		this.id = id;
		
		this.builder = new GroupBuilder(id);
	}
	public Template(String templateNsName,int id,String dictionaryAttr) {
		this.templateName = templateNsName;
		this.id = id;
		this.dictionaryAttr = dictionaryAttr;
		
		this.builder = new GroupBuilder(id);
	}
	public Template(String templateNsName,int id,String dictionaryAttr,String typeRef) {
		this.templateName = templateNsName;
		this.id = id;
		this.dictionaryAttr = dictionaryAttr;
		this.typeRef = typeRef;
		
		this.builder = new GroupBuilder(id);
	}
			
	public Template appendInteger(int id, Necessity presence, Operator operator, Integer type) {
		builder.addField(operator, TypeMask.IntegerSigned, presence, id);
		return this;
	}
	
	public Template appendDecimal(int id, Necessity presence, Operator exponentOperator, Operator mantissaOperator) {
		
	//	builder.addField(exponentOperator, FieldType.Scaled, presence, id);
	//	builder.addField(mantissaOperator, FieldType.Scaled, presence, id);
				
		return this;
	}
	
	public Template appendASCIIString(int id, Necessity presence, Operator operator) {
		builder.addField(operator, TypeMask.TextASCII, presence, id);
		return this;
	}
	
	public Template appendUnicodeString(int id, Necessity presence, Operator operator) {
		builder.addField(operator, TypeMask.TextUTF8, presence, id);
		return this;
	}
	
	public Template appendByteVectorField(int id, Necessity presence, Operator operator) {
		builder.addField(operator, TypeMask.ByteArray, presence, id);
		return this;
	}
	
	public Sequence appendSequence(int id, Necessity presence, String dictionary, String typeRef) {
		
		Sequence sequence = new Sequence(this, id, presence, dictionary, typeRef);
		
		return sequence;
	}
	
	public Group appendGroup(int id, Necessity presence, String dictionary, String typeRef) {
		
		Group group = new Group(this, id, presence, dictionary, typeRef);
		
		return group;
	}
	
	
	
	
	
	public Template appendTemplateRef() {
		//TODO: not sure what to do here.
		return this;
	}
	
	
	//Template
	//Instruction
	
	// new Template(dictionaryid, int, String).
	//  add uint32 mandatory/optional id name  operation
	//append(Type,Presence,int,String,operation) //type may be sequence or group? but need closing?
	
	
}
