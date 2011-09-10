package com.orbious.util.tokyo;

import java.io.File;
import com.orbious.util.config.Config;
import com.orbious.util.config.ConfigException;

public class HDBFile {

  protected final File filestore;
  private final int tokyoSize;
  private final boolean readOnly;

//  protected HDBWrapper hdbw;

  public HDBFile(File filestore, int tokyoSize, boolean readOnly) {
    this.filestore = filestore;
    this.tokyoSize = tokyoSize;
    this.readOnly = readOnly;
  }

  public HDBFile(String name, int tokyoSize, boolean readOnly) {
    this(new File(name), tokyoSize, readOnly);
  }

//  public boolean exists() {
//    if ( filestore == null ) {
//      return false;
//    }
//
//    return filestore.exists();
//  }
//
//  public void open() throws FileException {
//    if ( hdbw != null ) {
//      return;
//    }
//
//    hdbw = new HDBWrapper(filestore, tokyoSize, readOnly);
//    try {
//      hdbw.open();
//    } catch ( WrapperException we ) {
//      throw new FileException("Failed to open sentence file" + filestore, we);
//    }
//  }
//
//  public void close() throws FileException {
//    Exception e = null;
//
//    if ( !hdbw.readOnly() ) {
//      try {
//        hdbw.write(Config.stored_key, Config.xmlstr());
//      } catch ( ConfigException ce ) {
//        e = ce;
//      } catch ( WrapperException hwe ) {
//        e = hwe;
//      }
//    }
//
//    try {
//      hdbw.close();
//    } catch ( WrapperException hwe ) {
//      throw new FileException("Error closing hdb file " + filestore, hwe);
//    }
//
//    if ( e != null ) {
//      throw new FileException("Failed to write config to " +
//          filestore.toString(), e);
//    }
//  }
//
//  public String cfgstr() {
//    return (String)hdbw.readObject(Config.stored_key);
//  }
//
//  public HDBWrapper hdbw() {
//    return hdbw;
//  }
//
//  @Override
//  public String toString() {
//    return filestore.toString();
//  }
}
