package com.orbious.util.tokyo;

import java.io.File;
import com.orbious.util.config.Config;
import com.orbious.util.config.ConfigException;

public class HDBFile {

  protected final File file;
  protected HDBWrapper hdbw;

  public HDBFile(File file) {
    this.file = file;
  }

  public HDBFile(String name) {
    this.file = new File(name);
  }

  public boolean exists() {
    if ( file == null ) {
      return false;
    }

    return file.exists();
  }

  public void init(boolean readOnly) throws HDBFileException {
    if ( hdbw != null ) {
      return;
    }

    hdbw = new HDBWrapper();

    try {
      if ( readOnly ) {
        hdbw.initReader(file, 10000);
      } else {
        hdbw.initWriter(file, 1);
      }
    } catch ( WrapperException we ) {
      throw new HDBFileException("Failed to open sentence file" + file, we);
    }
  }

  public void close() throws HDBFileException {
    Exception e = null;

    if ( !hdbw.readOnly() ) {
      try {
        hdbw.write(Config.stored_key, Config.xmlstr());
      } catch ( ConfigException ce ) {
        e = ce;
      } catch ( WrapperException we ) {
        e = we;
      }
    }

    try {
      hdbw.close();
    } catch ( WrapperException we ) {
      throw new HDBFileException("Error closing sentence file " + file, we);
    }

    if ( e != null ) {
      throw new HDBFileException("Failed to write config to " + file.toString(), e);
    }
  }

  public String cfgstr() {
    return (String)hdbw.readObject(Config.stored_key);
  }

  public void iterinit() {
    hdbw.iterinit();
  }

  public byte[] iternext() {
    return hdbw.iternext();
  }

  @Override
  public String toString() {
    return file.toString();
  }
}
