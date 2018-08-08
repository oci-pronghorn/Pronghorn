package com.ociweb.json.decode;

import com.ociweb.json.JSONAccumRule;
import com.ociweb.json.JSONAligned;
import com.ociweb.json.JSONExtractorImpl;
import com.ociweb.json.JSONRequired;
import com.ociweb.json.JSONType;

public abstract class JSONTable<P> {
    final JSONExtractorImpl extractor;

    JSONTable(JSONExtractorImpl extractor) {
        this.extractor = extractor;
    }
    
    public <T extends Enum<T>> JSONTable<P> integerField(String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeInteger, JSONAligned.UNPADDED, null);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> stringField(String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeString, JSONAligned.UNPADDED, null);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> decimalField(String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeDecimal, JSONAligned.UNPADDED, null);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> booleanField(String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeBoolean, JSONAligned.UNPADDED, null);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }

    public <T extends Enum<T>> JSONTable<P> integerField(String extractionPath, T field, JSONRequired required, Object validator) {
    	extractor.newPath(JSONType.TypeInteger, JSONAligned.UNPADDED, null);
    	extractor.completePath(extractionPath, field.name(), field, required, validator);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> stringField(String extractionPath, T field, JSONRequired required, Object validator) {
    	extractor.newPath(JSONType.TypeString, JSONAligned.UNPADDED, null);
    	extractor.completePath(extractionPath, field.name(), field, required, validator);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> decimalField(String extractionPath, T field, JSONRequired required, Object validator) {
    	extractor.newPath(JSONType.TypeDecimal, JSONAligned.UNPADDED, null);
    	extractor.completePath(extractionPath, field.name(), field, required, validator);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> booleanField(String extractionPath, T field, JSONRequired required) {
    	extractor.newPath(JSONType.TypeBoolean, JSONAligned.UNPADDED, null);
    	extractor.completePath(extractionPath, field.name(), field, required, null);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> integerField(JSONAligned isAligned, JSONAccumRule accumRule, String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeInteger, isAligned, accumRule);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> stringField(JSONAligned isAligned, JSONAccumRule accumRule, String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeString, isAligned, accumRule);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> decimalField(JSONAligned isAligned, JSONAccumRule accumRule, String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeDecimal, isAligned, accumRule);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> booleanField(JSONAligned isAligned, JSONAccumRule accumRule, String extractionPath, T field) {
    	extractor.newPath(JSONType.TypeBoolean, isAligned, accumRule);
    	extractor.completePath(extractionPath, field.name(), field);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> integerField(JSONAligned isAligned, JSONAccumRule accumRule, String extractionPath, T field, JSONRequired required, Object validator) {
    	extractor.newPath(JSONType.TypeInteger, isAligned, accumRule);
    	extractor.completePath(extractionPath, field.name(), field, required, validator);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> stringField(JSONAligned isAligned, JSONAccumRule accumRule, String extractionPath, T field, JSONRequired required, Object validator) {
    	extractor.newPath(JSONType.TypeString, isAligned, accumRule);
    	extractor.completePath(extractionPath, field.name(), field, required, validator);    	
    	return this;
    }
    
    public <T extends Enum<T>> JSONTable<P> decimalField(JSONAligned isAligned, JSONAccumRule accumRule, String extractionPath, T field, JSONRequired required, Object validator) {
    	extractor.newPath(JSONType.TypeDecimal, isAligned, accumRule);
    	extractor.completePath(extractionPath, field.name(), field, required, validator);    	
    	return this;
    }
    
    
    public <T extends Enum<T>> JSONTable<P> booleanField(JSONAligned isAligned, JSONAccumRule accumRule, String extractionPath, T field, JSONRequired required) {
    	extractor.newPath(JSONType.TypeBoolean, isAligned, accumRule);
    	extractor.completePath(extractionPath, field.name(), field, required, null);    	
    	return this;
    }
    
    public P finish() {
        return tableEnded();
    }

    abstract P tableEnded();


}

