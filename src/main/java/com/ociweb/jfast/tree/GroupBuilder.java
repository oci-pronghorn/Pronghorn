package com.ociweb.jfast.tree;

import java.util.ArrayList;
import java.util.List;

import com.ociweb.jfast.Operator;
import com.ociweb.jfast.ValueDictionary;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.field.FieldGroupNoPMAP;
import com.ociweb.jfast.read.FieldType;

public class GroupBuilder {

	private final int id;
	
	private int mandatoryBitCount;
	
	private List<Operator> operators;
	private List<Necessity> necessity;
	private List<FieldType> types;
	private List<Integer> ids;
	private List<Byte> noPmapList;
	private ValueDictionary dictionary;
	
	public GroupBuilder(int id, ValueDictionary dictionary) {
		this.id = id;
		this.dictionary = dictionary;
		this.operators = new ArrayList<Operator>();
		this.necessity = new ArrayList<Necessity>();
		this.types = new ArrayList<FieldType>();
		this.ids = new ArrayList<Integer>();
		this.noPmapList   = new ArrayList<Byte>();
	}
	
	///Necessity, Required
	public void addField(Operator operator, FieldType type, Necessity necessity, int id) {
		
		//common logic to determine.
		// 1. if this field will have a bit in the pmap
		// 2. if this field will be nullable
		
		int noPMap = 0; //this is mandatory from before.
		if (operator==Operator.None || operator==Operator.Delta) {
			noPMap = 1;
		}
		
		//If NoOperator eg None we have NO PMAP bit
		//If operator is Constant and mandatory we have no pmap.
		//if Delta we have no pmap bit.
		switch (necessity) {
			case mandatory: //Never be null
				if (operator==Operator.Constant) {
					noPMap = 1;
				}
				break;
			case optional:
				//Nullable unless constant operator
				//constant values are never xmitted and never nullable
				//this is a mutually exclusive relationship which
				//eliminates the need to have NullsConst form of each field
				if (operator==Operator.Constant) {
					//changed to mandatory because its a "Constant"
					necessity = Necessity.mandatory;
				}
				break;
		}
		
		this.mandatoryBitCount+=noPMap;
		
		this.necessity.add(necessity);
		this.operators.add(operator);
		this.types.add(type);
		this.ids.add(id);
		this.noPmapList.add((byte)noPMap);
	}

	
	private ValueDictionaryEntry dictionaryEntry(int i) {
		return dictionary.entry[i];//TODO: need more complex logic here
	}

	
	public Field buildSimpleGroup() {
		
		Field[] fields = simpleFieldArray();
		
		return new FieldGroupNoPMAP(fields);
		
	}

	public Field[] simpleFieldArray() {
		
		///TODO: if fields repeat the same then we wil add a count to avoid the virtual call
		///TODO: confirm error if 2 fields have same name with different compression scheme.
		///TODO: store fields in lookuptable for re-use by name/namespace (may need first pass to find repeats that cant run)
		///TODO: once product is complete review building composite fields of multiple types to remove function calls
		
		Field[] fields = new Field[types.size()];
		int c = 0;
		int j = types.size();
		while (c<types.size()) {
			Field f = types.get(c).newField(ids.get(c), dictionaryEntry(ids.get(c)), operators.get(c), necessity.get(c));
			fields[--j] = f;
			c++;
		}
		return fields;
	}

	
	
}
