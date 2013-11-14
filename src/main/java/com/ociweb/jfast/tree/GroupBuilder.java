package com.ociweb.jfast.tree;

import java.util.ArrayList;
import java.util.List;

import com.ociweb.jfast.Operator;
import com.ociweb.jfast.field.util.ValueDictionary;

public class GroupBuilder {

	private final int id;
	
	private int mandatoryBitCount;
	
	private List<Operator> operators;
	private List<Necessity> necessity;
	private List<Integer> types;
	private List<Integer> ids;
	private List<Byte> noPmapList;
	private ValueDictionary dictionary;
	
	public GroupBuilder(int id, ValueDictionary dictionary) {
		this.id = id;
		this.dictionary = dictionary;
		this.operators = new ArrayList<Operator>();
		this.necessity = new ArrayList<Necessity>();
		this.types = new ArrayList<Integer>();
		this.ids = new ArrayList<Integer>();
		this.noPmapList   = new ArrayList<Byte>();
	}
	
	///Necessity, Required
	public void addField(Operator operator, int type, Necessity necessity, int id) {
		
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

	

	
	
}
