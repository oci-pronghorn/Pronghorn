package com.ociweb.jfast.catalog.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupGenerator implements ItemGenerator {

    private final String name;
    private final int id;
    private final boolean presence;
    
    List<ItemGenerator> items = new ArrayList<ItemGenerator>();
	
    public GroupGenerator(String name, int id, boolean presence) {
    	this.name = name;
    	this.id = id;
        this.presence = presence;
    	this.items = new ArrayList<ItemGenerator>();
    }
    
    public GroupGenerator(String name, int id, boolean presence, List<ItemGenerator> items) {
    	this.name = name;
    	this.id = id;
        this.presence = presence;
    	this.items = items;
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
        openGroup(result,name, id, presence);
                
        String innerTab = null==tab ? "" : tab+"    ";
        for(ItemGenerator item:items) {
            item.appendTo(innerTab,result);
        }
        result.append(tab);
        closeGroup(result);
        return result;
    }

    public static void closeGroup(Appendable result) throws IOException {
        result.append("</group>\n");
    }

    public static void openGroup(Appendable result, String name, int id, boolean presence) throws IOException {
        result.append("<group ");
        if (null!=name) {
        	result.append("name=\"").append(name).append("\" ");
        }
            result.append("id=\"").append(Integer.toString(id)).append("\" ");
        
        if (presence) {
            result.append("presence=\"optional\" ");
        }        
        result.append(">\n");
    }

}
