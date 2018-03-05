package com.ociweb.json.encode;

import com.ociweb.json.appendable.StringBuilderWriter;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONRootDoubleTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testRootDecimal() {
        JSONRenderer<Double> json = new JSONRenderer<Double>()
                .decimal(2, o -> o);
        assertTrue(json.isLocked());
        json.render(out, 9.0);
        assertEquals("9.00", out.toString());
    }

    @Test
    public void testRootNullableDecimal_NotNull() {
        JSONRenderer<Double> json = new JSONRenderer<Double>()
                .nullableDecimal(2, Objects::isNull, o -> o);
        assertTrue(json.isLocked());
        json.render(out, 9.0);
        assertEquals("9.00", out.toString());
    }

    @Test
    public void testRootNullableDecimal_Null() {
        JSONRenderer<Double> json = new JSONRenderer<Double>()
                .nullableDecimal(2, Objects::isNull, o -> o);
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootDecimal_Zero() {
        JSONRenderer<Double> json = new JSONRenderer<Double>()
                .decimal(2, o -> o);
        assertTrue(json.isLocked());
        json.render(out, 0.0);
        assertEquals("0.00", out.toString());
    }

    @Test
    public void testRootDecimal_NoPrecision() {
        JSONRenderer<Double> json = new JSONRenderer<Double>()
                .decimal(0, o -> o);
        assertTrue(json.isLocked());
        json.render(out, 9.3);
        assertEquals("9", out.toString());
    }

    @Test
    public void testRootDecimal_NoPrecisionZero() {
        JSONRenderer<Double> json = new JSONRenderer<Double>()
                .decimal(0, o -> o);
        assertTrue(json.isLocked());
        json.render(out, 0.0);
        assertEquals("0", out.toString());
    }

    @Test
    public void testRootDecimal_Fract() {
        JSONRenderer<Double> json = new JSONRenderer<Double>()
                .decimal(2, o -> o);
        assertTrue(json.isLocked());
        json.render(out, 0.321);
        assertEquals("0.32", out.toString());
    }

    @Test
    public void testRootDecimalD() {
        JSONRenderer<Double> json = new JSONRenderer<Double>()
                .decimal((o, v) -> v.visit(o, 2));
        assertTrue(json.isLocked());
        json.render(out, 9.0);
        assertEquals("9.00", out.toString());
    }

    @Test
    public void testRootDecimal_ZeroD() {
        JSONRenderer<Double> json = new JSONRenderer<Double>()
                .decimal((o, v) -> v.visit(o, 2));
        assertTrue(json.isLocked());
        json.render(out, 0.0);
        assertEquals("0.00", out.toString());
    }

    @Test
    public void testRootDecimal_NoPrecisionD() {
        JSONRenderer<Double> json = new JSONRenderer<Double>()
                .decimal((o, v) -> v.visit(o, 0));
        assertTrue(json.isLocked());
        json.render(out, 9.3);
        assertEquals("9", out.toString());
    }

    @Test
    public void testRootDecimal_NoPrecisionZeroD() {
        JSONRenderer<Double> json = new JSONRenderer<Double>()
                .decimal((o, v) -> v.visit(o, 0));
        assertTrue(json.isLocked());
        json.render(out, 0.0);
        assertEquals("0", out.toString());
    }

    @Test
    public void testRootDecimal_FractD() {
        JSONRenderer<Double> json = new JSONRenderer<Double>()
                .decimal((o, v) -> v.visit(o, 2));
        assertTrue(json.isLocked());
        json.render(out, 0.321);
        assertEquals("0.32", out.toString());
    }
}
