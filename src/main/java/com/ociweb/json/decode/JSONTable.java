package com.ociweb.json.decode;

import com.ociweb.json.JSONAccumRule;
import com.ociweb.json.JSONExtractorImpl;
import com.ociweb.json.JSONExtractorActive;
import com.ociweb.json.JSONType;

public abstract class JSONTable<P> {
    final JSONExtractorImpl extractor;

    JSONTable(JSONExtractorImpl extractor) {
        this.extractor = extractor;
    }
    
    public <T extends Enum<T>> JSONTable<P> integerField(String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeInteger, false, null);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> stringField(String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeString, false, null);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> decimalField(String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeDecimal, false, null);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> booleanField(String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeBoolean, false, null);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> integerField(boolean isAligned, JSONAccumRule accumRule, String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeInteger, isAligned, accumRule);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> stringField(boolean isAligned, JSONAccumRule accumRule, String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeString, isAligned, accumRule);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> decimalField(boolean isAligned, JSONAccumRule accumRule, String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeDecimal, isAligned, accumRule);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> booleanField(boolean isAligned, JSONAccumRule accumRule, String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeBoolean, isAligned, accumRule);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public P finish() {
        return tableEnded();
    }

    abstract P tableEnded();
    
    //////////////////// remove old APIs below here

    @Deprecated
    public JSONPath<JSONTable<P>> element(JSONType type) {
        return element(type, false, null);
    }

    @Deprecated
    public JSONPath<JSONTable<P>> element(JSONType type, boolean isAligned) {
        return element(type, isAligned, null);
    }

    @Deprecated
    public JSONPath<JSONTable<P>> element(JSONType type, boolean isAligned, JSONAccumRule accumRule) {
        JSONExtractorActive active = extractor.newPath(type, isAligned, accumRule);
        return new JSONPath<JSONTable<P>>(active) {
            @Override
            JSONTable <P> pathEnded() {
                return JSONTable.this;
            }
        };
    }

}

