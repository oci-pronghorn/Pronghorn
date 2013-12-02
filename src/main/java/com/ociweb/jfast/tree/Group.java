package com.ociweb.jfast.tree;

import java.util.Iterator;

public class Group {

	//single FASTManger will keep the arrays with default values populated
	//This group class only holds the needed data and will be able to 
	//populate part of that list upon request.  This call is cascaded from Template.
	
	
	protected Group() {
		
	}
	
	public Group(Iterator attributes) {
		// TODO Auto-generated constructor stub
	}

	public void append(int type, int operation, String value, String id, 
			            String name, String key, String dictionary, String namespace) {
		
		//grow array as needed 
		//combine type/operation/index into token
		//value must be used to set default value
		//name/namespace combo is a unique id for this field
		//id is another id for this field
		
		//the instance id should be generated outside this method and passed in.
		//along with the dictionary id.  key allows us to share with another instance id.
		//dictionary and key are the harder fields to reason about.
		//template - same dictionary iff same template id.
		//type - same dictionar iff templates have same application id.
		//global - default
		//custom - create dictionary based on unique string.
		
		
	}

	public void append(Group target) {
		// TODO Auto-generated method stub
		
	}


}
