package com.ociweb.jfast.catalog.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.ociweb.pronghorn.ring.token.OperatorMask;

public class SequenceGenerator implements ItemGenerator {

    private final String name;
    private final int id;
    private final String lengthName;
    private final int lengthId;
    private final int lengthOperator;
    
    List<ItemGenerator> items = new ArrayList<ItemGenerator>();
	
    public SequenceGenerator(String name, int id, String lengthName, int lengthId, int lengthOperator) {
    	this.name = name;
    	this.id = id;
        this.lengthName = lengthName;
        this.lengthId = lengthId;
    	this.items = new ArrayList<ItemGenerator>();
    	this.lengthOperator = lengthOperator;
    }
    
    public SequenceGenerator(String name, int id, String lengthName, int lengthId, int lengthOperator, List<ItemGenerator> items) {
    	this.name = name;
    	this.id = id;
        this.lengthName = lengthName;
        this.lengthId = lengthId;
    	this.items = items;
    	this.lengthOperator = lengthOperator;
    }

    public String toString() {
        try {
			return appendTo("",new StringBuilder()).toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }
    
    @Override
    public Appendable appendTo(String tab, Appendable result) throws IOException {
        
        result.append(tab);
        openSequence(result,name, id, lengthName, lengthId, lengthOperator);
                
        String innerTab = null==tab ? "" : tab+"    ";
        for(ItemGenerator item:items) {
            item.appendTo(innerTab,result);
        }
        result.append(tab);
        closeSequence(result);
        return result;
    }

    public static void closeSequence(Appendable result) throws IOException {
        result.append("</sequence>\n");
    }

    public static void openSequence(Appendable result, String name, int id, String lengthName, int lengthId, int lengthOperator) throws IOException {
        result.append("<sequence ");
        if (null!=name) {
        	result.append("name=\"").append(name).append("\" ");
        }
        result.append("id=\"").append(Integer.toString(id)).append("\" ");             
        result.append(">\n");
        
        
        result.append("<length ");
        if (null!=name) {
        	result.append("name=\"").append(lengthName).append("\" ");
        }
        result.append("id=\"").append(Integer.toString(lengthId)).append("\" ");   
        
        
        if (OperatorMask.Field_None != lengthOperator) {
        	result.append(">\n");
        	
          //  result.append(tab);
            result.append("<").append(OperatorMask.xmlOperatorName[lengthOperator]);
            
//            if (null!=initial) { 
//                result.append(" value=\"").append(initial).append("\"");
//             } 
            result.append("/>\n");
            
            result.append("</length>");
        } else {
        
        	result.append(">\n");
        }
        
    }

}
