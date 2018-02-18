package com.ociweb.json.structure.annotations;

import java.io.IOException;

// TODO: use DataOutput or somewthing similar to remove the toString calls and allow a channel to be the desination
// TODO: create another imnpl for JSON output

public class ProngStructFormatter {
    final StringBuilder indent = new StringBuilder();
    private final Appendable sb;
    int indentStrLen = 0;

    public ProngStructFormatter(Appendable appendable) {
        this.sb = appendable;
    }

    public ProngStructFormatter() {
        this(new StringBuilder());
    }

    @Override
    public String toString() {
        return sb.toString();
    }

    public void beginScope(String type) {
        append("<");
        append(type);
        append("> {\n");
        String indentStr = "\t";
        indentStrLen = indentStr.length();
        indent.append(indentStr);
    }

    public void endScope() {
        int len = indent.length();
        indent.delete(len - indentStrLen, len);
        append(indent);
        append('}');
    }

    public void assignment(CharSequence member, CharSequence value) {
        append(indent);
        append(member);
        if (value == null) {
            append(" = null\n");
        }
        else {
            append(value);
            append("\n");
        }
    }

    public void assignment(CharSequence member, Object boxedValue) {
        append(indent);
        append(member);
        if (boxedValue == null) {
            append(" = null\n");
        }
        else {
            append(" = ");
            append(boxedValue.toString());
            append("\n");
        }
    }

    public void assignment(CharSequence member, boolean value, boolean isNull) {
        append(indent);
        append(member);
        if (isNull) {
            append(" = null\n");
        }
        else {
            append(" = ");
            append(Boolean.toString(value));
            append("'\n");
        }
    }

    public void assignment(CharSequence member, char value, boolean isNull) {
        append(indent);
        append(member);
        if (isNull) {
            append(" = null\n");
        }
        else {
            append(" = '");
            append(value);
            append("'\n");
        }
    }

    public void assignment(CharSequence member, long value, boolean isNull) {
        append(indent);
        append(member);
        append(" = ");
        if (isNull) {
            append(" = null\n");
        }
        else {
            append(Long.toString(value));
            append("\n");
        }
    }

    public void assignment(CharSequence member, float value, boolean isNull) {
        append(indent);
        append(member);
        if (isNull) {
            append(" = null\n");
        }
        else {
            append(" = ");
            append(Float.toString(value));
            append("\n");
        }
    }

    public void assignment(CharSequence member, double value, boolean isNull) {
        append(indent);
        append(member);
        if (isNull) {
            append(" = null\n");
        }
        else {
            append(" = ");
            append(Double.toString(value));
            append("\n");
        }
    }

    public void assignment(CharSequence member, ProngStructFormatting value) {
        assignment(member, value, false);
    }

    public void assignment(CharSequence member, ProngStructFormatting value, boolean isNull) {
        append(indent);
        append(member);
        if (value == null || isNull) {
            append(" = null\n");
        }
        else {
            append(" = ");
            value.toString(this);
            append("\n");
        }
    }

    private void append(CharSequence str) {
        try {
            sb.append(str);
        } catch (IOException ignored) {
        }
    }

    private void append(char c) {
        try {
            sb.append(c);
        } catch (IOException ignored) {
        }
    }
}
