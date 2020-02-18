package org.polypheny.db;


import java.io.Serializable;


/**
 * Base class for a CatalogEntry
 */
public abstract class CatalogEntry implements Serializable {

    private final String name;


    CatalogEntry( String name ) {
        this.name = name;
    }


    public String getName() {
        return name;
    }


    abstract byte[] getBytes();


}
