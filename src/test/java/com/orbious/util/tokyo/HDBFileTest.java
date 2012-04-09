package com.orbious.util.tokyo;

import java.io.File;

import com.orbious.util.config.Config;
import com.orbious.util.config.ConfigException;
import com.orbious.util.config.IConfig;
import com.orbious.util.tokyo.HDBFile;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class HDBFileTest {

  @Before
  public void before() {
    try {
      Config.setDefaults(TestConfig.class);
    } catch ( ConfigException ce ) {
      System.err.println("Error during config initialisation");
      ce.printStackTrace();
    }
  }

  @Test
  public void add() throws Exception {
    File f = File.createTempFile("HDBFileTest", ".hdb");
    HDBFile h = new HDBFile(f, 10, false);
    h.open();
    for ( int i = 0; i < 100; i++ )
      h.write(i, i);

    for ( int i = 0; i < 100; i++ )
      assertThat(h.readInt(i), is(equalTo(i)));

    h.close();
    h.open();

    for ( int i = 0; i < 100; i++ )
      assertThat(h.readInt(i), is(equalTo(i)));
  }

  @Test
  public void writecfg() throws Exception {
    File f = File.createTempFile("HDBFileTest", ".hdb");

    HDBFile h = new HDBFile(f, 10, false);
    h.open();
    h.writecfg();
    h.close();

    h.open();

    assertThat(h.cfg(TestConfig.app_version), is(equalTo("5.0")));
    assertThat(h.cfg(TestConfig.log_realm), is(equalTo("sentence-extractor")));
  }

	 enum TestConfig implements IConfig {
	    app_version("5.0"),
	    log_realm("sentence-extractor"),
	    log_config("com/orbious/extractor/log4j.xml");

	    private String svalue = null;
	    private int ivalue = -1;
	    private float fvalue = Float.NaN;
	    private double dvalue = Double.NaN;
	    private long lvalue = -1;
	    private boolean bvalue = false;

	    private TestConfig(String value) { this.svalue = value; }
	    private TestConfig(int value) { this.ivalue = value; }
	    private TestConfig(double value) { this.dvalue = value; }
	    private TestConfig(long value) { this.lvalue = value; }
	    private TestConfig(boolean value) { this.bvalue = value; }

	    public boolean isString() { return (svalue != null) ? true : false; }
	    public String asString() { return svalue; }

	    public boolean isInt() { return (ivalue != -1) ? true : false; }
	    public int asInt() { return ivalue; }

	    public boolean isFloat() { return (fvalue != Float.NaN) ? true : false; }
	    public float asFloat() { return fvalue; }

	    public boolean isDouble() { return (dvalue != Double.NaN) ? true : false; }
	    public double asDouble() { return dvalue; }

	    public boolean isLong() { return (lvalue != -1) ? true : false; }
	    public long asLong() { return lvalue; }

	    public boolean isBool() { return true; }
	    public boolean asBool() { return bvalue; }

	    public String getName() { return this.name(); }
	  }

}
