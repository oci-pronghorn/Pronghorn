package com.ociweb.json.encode;

import com.ociweb.json.appendable.StringBuilderWriter;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONRootBoolTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testEmpty() {
        JSONRenderer<Object> json = new JSONRenderer<>().empty();
        assertTrue(json.isLocked());
        json.render(out, new Object());
        assertEquals("", out.toString());
    }

    @Test
    public void testRootConstantNull() {
        JSONRenderer<Object> json = new JSONRenderer<>()
                .constantNull();
        assertTrue(json.isLocked());
        json.render(out, new Object());
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootBoolTrue() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .bool(o -> o);
        assertTrue(json.isLocked());
        json.render(out, true);
        assertEquals("true", out.toString());
    }

    @Test
    public void testRootBoolFalse() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .bool(o -> o);
        assertTrue(json.isLocked());
        json.render(out, false);
        assertEquals("false", out.toString());
    }

    @Test
    public void testRootBoolNul_lNull() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .nullableBool(Objects::isNull, o -> o);
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootBoolNull_True() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .nullableBool(Objects::isNull, o -> o);
        assertTrue(json.isLocked());
        json.render(out, true);
        assertEquals("true", out.toString());
    }

    @Test
    public void testRootBoolNull_False() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .nullableBool(Objects::isNull, o -> o);
        assertTrue(json.isLocked());
        json.render(out, false);
        assertEquals("false", out.toString());
    }
}
