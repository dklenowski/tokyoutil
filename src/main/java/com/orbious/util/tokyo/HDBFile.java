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

    hdbw = new HDBWrapper(file, 10000);

    try {
      if ( readOnly ) {
        hdbw.initReader();
      } else {
        hdbw.initWriter();
      }
    } catch ( HDBWrapperException we ) {
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
      } catch ( HDBWrapperException hwe ) {
        e = hwe;
      }
    }

    try {
      hdbw.close();
    } catch ( HDBWrapperException hwe ) {
      throw new HDBFileException("Error closing sentence file " + file, hwe);
    }

    if ( e != null ) {
      throw new HDBFileException("Failed to write config to " + file.toString(), e);
    }
  }

  public String cfgstr() {
    return (String)hdbw.readObject(Config.stored_key);
  }

  public HDBWrapper hdbw() {
    return hdbw;
  }

  @Override
  public String toString() {
    return file.toString();
  }
}
