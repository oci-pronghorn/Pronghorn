package com.ociweb.jfast.catalog.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TemplateGenerator implements ItemGenerator {

    private final String name;
    private final int id;
    private final boolean reset;
    private final String dictionary;
    
    List<ItemGenerator> items = new ArrayList<ItemGenerator>();
    
    public TemplateGenerator(String name, int id, boolean reset, String dictionary) {
        this.name = name;
        this.id = id;
        this.reset = reset;
        this.dictionary = dictionary;
        this.items = new ArrayList<ItemGenerator>();
    }
    
    public TemplateGenerator(String name, int id, boolean reset, String dictionary, List<ItemGenerator> items) {
        this.name = name;
        this.id = id;
        this.reset = reset;
        this.dictionary = dictionary;
        this.items = items;
    }
        
    
    public String toString() {
        try {
			return appendTo("",new StringBuilder()).toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }
    
    public SequenceGenerator addSequence(String name, int id, String lengthName, int lengthId, int lengthOperator) {
        SequenceGenerator field = new SequenceGenerator(name, id, lengthName, lengthId, lengthOperator);
        items.add(field);
        return field;
    }
    
    public GroupGenerator addGroup(String name, int id, boolean presence) {
        GroupGenerator group = new GroupGenerator(name,id,presence);
        items.add(group);
        return group;
    }
    
    public FieldGenerator addField(String name, int id, boolean presence, int type, int operator, String initial) {
        FieldGenerator field = new FieldGenerator(name,id,presence,type,operator,initial);
        items.add(field);
        return field;
    }
    
    public FieldGenerator addField(String name, int id, boolean presence, int type, int operator1, int operator2, String initial1, String initial2) {
        FieldGenerator field = new FieldGenerator(name,id,presence,type,operator1, operator2, initial1, initial2);
        items.add(field);
        return field;
    }   
    

    public Appendable appendTo(String tab, Appendable result) throws IOException {
                
        result.append(tab);
        openTemplate(result,name, id, reset, dictionary);
                
        String innerTab = null==tab ? "" : tab+"    ";
        for(ItemGenerator item:items) {
            item.appendTo(innerTab,result);
        }
        result.append(tab);
        closeTemplate(result);
        return result;
    }

    public static void closeTemplate(Appendable result) throws IOException {
        result.append("</template>\n");
    }

    public static void openTemplate(Appendable result, String name, long id, boolean reset, String dictionary) throws IOException {
        result.append("<template ");
        if (null!=name) {
        	result.append("name=\"").append(name).append("\" ");
        }
        result.append("id=\"").append(Long.toString(id)).append("\" ");
        if (reset) {
            result.append("reset=\"").append("Y").append("\" ");
        }
        if (null!=dictionary) {
            result.append("dictionary=\"").append(dictionary).append("\" ");
        }
        result.append("xmlns=\"http://www.fixprotocol.org/ns/fast/td/1.1\">\n");
    }
    

}
