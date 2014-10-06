package com.ociweb.jfast.catalog.extraction;

public class ExtractorWorkspace {
    public boolean inQuote;
    public boolean inEscape;
    public int contentPos;
    public boolean contentQuoted;
    private int recordStart;
       
    
    public ExtractorWorkspace(boolean inQuote, boolean inEscape, int contentPos, boolean contentQuoted, int recordStart) {
        this.inQuote = inQuote;
        this.inEscape = inEscape;
        this.contentPos = contentPos;
        this.contentQuoted = contentQuoted;
        this.setRecordStart(recordStart);
    }

    public void reset() {
        inQuote = false;
        inEscape = false; 
        contentPos = -1;
        contentQuoted = false;
        setRecordStart(0);
    }

    public int getRecordStart() {
        return recordStart;
    }

    public void setRecordStart(int recordStart) {
        this.recordStart = recordStart;
    }
}