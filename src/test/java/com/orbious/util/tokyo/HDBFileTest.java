package com.orbious.util.tokyo;

import com.orbious.util.config.IConfig;
import com.orbious.util.tokyo.HDBFile;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import org.junit.Test;
//import java.io.File;

public class HDBFileTest {

	@Test
	public void type() throws Exception {
		// TODO auto-generated by JUnit Helper.
		assertThat(HDBFile.class, notNullValue());
	}

//	@Test
//	public void instantiation() throws Exception {
//		// TODO auto-generated by JUnit Helper.
//		File filestore = null;
//		int tokyoSize = 0;
//		boolean readOnly = false;
//		HDBFile target = new HDBFile(filestore, tokyoSize, readOnly);
//		assertThat(target, notNullValue());
//	}
//
//	@Test
//	public void writecfg_A$() throws Exception {
//		// TODO auto-generated by JUnit Helper.
//		File filestore = null;
//		int tokyoSize = 0;
//		boolean readOnly = false;
//		HDBFile target = new HDBFile(filestore, tokyoSize, readOnly);
//		target.writecfg();
//	}
//
//	@Test
//	public void writecfg_A$_T$StorageException() throws Exception {
//		// TODO auto-generated by JUnit Helper.
//		File filestore = null;
//		int tokyoSize = 0;
//		boolean readOnly = false;
//		HDBFile target = new HDBFile(filestore, tokyoSize, readOnly);
//		try {
//			target.writecfg();
//			fail("Expected exception was not thrown!");
//		} catch (StorageException e) {
//		}
//	}
//
//	@Test
//	public void close_A$() throws Exception {
//		// TODO auto-generated by JUnit Helper.
//		File filestore = null;
//		int tokyoSize = 0;
//		boolean readOnly = false;
//		HDBFile target = new HDBFile(filestore, tokyoSize, readOnly);
//		target.close();
//	}
//
//	@Test
//	public void close_A$_T$StorageException() throws Exception {
//		// TODO auto-generated by JUnit Helper.
//		File filestore = null;
//		int tokyoSize = 0;
//		boolean readOnly = false;
//		HDBFile target = new HDBFile(filestore, tokyoSize, readOnly);
//		try {
//			target.close();
//			fail("Expected exception was not thrown!");
//		} catch (StorageException e) {
//		}
//	}
//
//	@Test
//	public void cfgstr_A$() throws Exception {
//		// TODO auto-generated by JUnit Helper.
//		File filestore = null;
//		int tokyoSize = 0;
//		boolean readOnly = false;
//		HDBFile target = new HDBFile(filestore, tokyoSize, readOnly);
//		String actual = target.cfgstr();
//		String expected = null;
//		assertThat(actual, is(equalTo(expected)));
//	}


	 enum TestConfig implements IConfig {
	    app_version("5.0"),
	    log_realm("sentence-extractor"),
	    log_config("com/orbious/extractor/log4j.xml");

	    private String svalue = null;
	    private int ivalue = -1;
	    private float fvalue = Float.NaN;
	    private double dvalue = Double.NaN;
	    private boolean bvalue = false;

	    private TestConfig(String value) { this.svalue = value; }
	    private TestConfig(int value) { this.ivalue = value; }
	    private TestConfig(double value) { this.dvalue = value; }
	    private TestConfig(boolean value) { this.bvalue = value; }

	    public boolean isString() { return (svalue != null) ? true : false; }
	    public String asString() { return svalue; }

	    public boolean isInt() { return (ivalue != -1) ? true : false; }
	    public int asInt() { return ivalue; }

	    public boolean isFloat() { return (fvalue != Float.NaN) ? true : false; }
	    public float asFloat() { return fvalue; }

	    public boolean isDouble() { return (dvalue != Double.NaN) ? true : false; }
	    public double asDouble() { return dvalue; }

	    public boolean isBool() { return true; }
	    public boolean asBool() { return bvalue; }

	    public String getName() { return this.name(); }
	  }

}
