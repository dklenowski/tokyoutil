package com.orbious.util.tokyo;

import java.io.File;
import com.orbious.util.config.Config;
import com.orbious.util.config.ConfigException;

public class HDBFile extends HDBStorage implements IFileStorage {

  public HDBFile(File filestore, int tokyoSize, boolean readOnly) {
    super(filestore, tokyoSize, readOnly);
  }

  /*
   * Interface methods.
   */

  public void writecfg() throws StorageException {
    String xmlstr;

    xmlstr = null;
    try {
      xmlstr = Config.xmlstr();
    } catch ( ConfigException ce ) {
      throw new StorageException("Failed to extract xml config", ce);
    }

    write(Config.config_hdb_key, xmlstr);
  }

  @Override
  public void close() throws StorageException {
    StorageException e = null;

    if ( !readOnly ) {
      try {
        writecfg();
      } catch ( StorageException se ) {
        e = se;
      }
    }

    super.close();

    if ( e != null ) {
      throw e;
    }
  }

  public String cfgstr() {
    return (String)readObject(Config.config_hdb_key);
  }
}
