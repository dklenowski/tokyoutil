package com.orbious.tokyo;

import com.orbious.tokyo.ByteUtils;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import org.junit.Test;

public class ByteUtilsTest {

	@Test
	public void type() throws Exception {
		assertThat(ByteUtils.class, notNullValue());
	}

	@Test
	public void instantiation() throws Exception {
		ByteUtils target = new ByteUtils();
		assertThat(target, notNullValue());
	}

	@Test
	public void ints() {
	  int actual, expected;
	  byte[] b;

    expected = Integer.MIN_VALUE;
    b = ByteUtils.intToBytes(expected);
    actual = ByteUtils.bytesToInt(b);
    assertThat(actual, is(equalTo(expected)));

    expected = Integer.MAX_VALUE;
    b = ByteUtils.intToBytes(expected);
    actual = ByteUtils.bytesToInt(b);
    assertThat(actual, is(equalTo(expected)));

	  expected = 167;
	  b = ByteUtils.intToBytes(expected);
	  actual = ByteUtils.bytesToInt(b);
	  assertThat(actual, is(equalTo(expected)));
	}

	@Test
	public void longs() {
	  long actual, expected;
	  byte[] b;

	  expected = Long.MIN_VALUE;
    b = ByteUtils.longToBytes(expected);
    actual = ByteUtils.bytesToLong(b);
    assertThat(actual, is(equalTo(expected)));

    expected = Long.MAX_VALUE;
    b = ByteUtils.longToBytes(expected);
    actual = ByteUtils.bytesToLong(b);
    assertThat(actual, is(equalTo(expected)));

    expected = 189L;
    b = ByteUtils.longToBytes(expected);
    actual = ByteUtils.bytesToLong(b);
    assertThat(actual, is(equalTo(expected)));
	}

	 @Test
	  public void doubles() {
	    double actual, expected;
	    byte[] b;

	    expected = Double.MIN_VALUE;
	    b = ByteUtils.doubleToBytes(expected);
	    actual = ByteUtils.bytesToDouble(b);
	    assertThat(actual, is(equalTo(expected)));

	    expected = Double.MAX_VALUE;
	    b = ByteUtils.doubleToBytes(expected);
	    actual = ByteUtils.bytesToDouble(b);
	    assertThat(actual, is(equalTo(expected)));

	    expected = 189L;
	    b = ByteUtils.doubleToBytes(expected);
	    actual = ByteUtils.bytesToDouble(b);
	    assertThat(actual, is(equalTo(expected)));
	  }

   @Test
   public void strings() {
     String actual, expected;
     byte[] b;

     expected = "a string";
     b = ByteUtils.strToBytes(expected);
     actual = ByteUtils.bytesToStr(b);
     assertThat(actual, is(equalTo(expected)));
   }
}
