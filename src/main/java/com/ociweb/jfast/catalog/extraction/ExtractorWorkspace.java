package com.ociweb.jfast.catalog.extraction;

public class ExtractorWorkspace {
    public boolean inQuote;
    public boolean inEscape;
    public int contentPos;
    public boolean contentQuoted;
    private int recordStart;
    private boolean inExcapeAtStart;
    public ExtractorWorkspace(boolean inQuote, boolean inEscape, int contentPos, boolean contentQuoted, int recordStart) {
        this.inQuote = inQuote;
        this.inEscape = inEscape;
        this.contentPos = contentPos;
        this.contentQuoted = contentQuoted;
        this.setRecordStart(recordStart);
    }

    public void reset() {
        inQuote = false;
        inEscape = inExcapeAtStart; 
        contentPos = -1;
        contentQuoted = false;
        setRecordStart(0);
    }

    public int getRecordStart() {
        return recordStart;
    }

    public void setRecordStart(int recordStart) {
        this.inExcapeAtStart = inEscape;
        this.recordStart = recordStart;
    }
}