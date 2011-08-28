package com.orbious.util.tokyo;

import com.orbious.util.tokyo.Bytes;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import org.junit.Test;

public class BytesTest {

	@Test
	public void type() throws Exception {
		assertThat(Bytes.class, notNullValue());
	}

	@Test
	public void instantiation() throws Exception {
		Bytes target = new Bytes();
		assertThat(target, notNullValue());
	}

	@Test
	public void ints() {
	  int actual, expected;
	  byte[] b;

    expected = Integer.MIN_VALUE;
    b = Bytes.intToBytes(expected);
    actual = Bytes.bytesToInt(b);
    assertThat(actual, is(equalTo(expected)));

    expected = Integer.MAX_VALUE;
    b = Bytes.intToBytes(expected);
    actual = Bytes.bytesToInt(b);
    assertThat(actual, is(equalTo(expected)));

	  expected = 167;
	  b = Bytes.intToBytes(expected);
	  actual = Bytes.bytesToInt(b);
	  assertThat(actual, is(equalTo(expected)));
	}

	@Test
	public void longs() {
	  long actual, expected;
	  byte[] b;

	  expected = Long.MIN_VALUE;
    b = Bytes.longToBytes(expected);
    actual = Bytes.bytesToLong(b);
    assertThat(actual, is(equalTo(expected)));

    expected = Long.MAX_VALUE;
    b = Bytes.longToBytes(expected);
    actual = Bytes.bytesToLong(b);
    assertThat(actual, is(equalTo(expected)));

    expected = 189L;
    b = Bytes.longToBytes(expected);
    actual = Bytes.bytesToLong(b);
    assertThat(actual, is(equalTo(expected)));
	}

	 @Test
	  public void doubles() {
	    double actual, expected;
	    byte[] b;

	    expected = Double.MIN_VALUE;
	    b = Bytes.doubleToBytes(expected);
	    actual = Bytes.bytesToDouble(b);
	    assertThat(actual, is(equalTo(expected)));

	    expected = Double.MAX_VALUE;
	    b = Bytes.doubleToBytes(expected);
	    actual = Bytes.bytesToDouble(b);
	    assertThat(actual, is(equalTo(expected)));

	    expected = 189L;
	    b = Bytes.doubleToBytes(expected);
	    actual = Bytes.bytesToDouble(b);
	    assertThat(actual, is(equalTo(expected)));
	  }

   @Test
   public void strings() {
     String actual, expected;
     byte[] b;

     expected = "a string";
     b = Bytes.strToBytes(expected);
     actual = Bytes.bytesToStr(b);
     assertThat(actual, is(equalTo(expected)));
   }
}
