package org.polypheny.db;


/**
 * Base class for a CatalogEntry
 */
public abstract class CatalogEntry {

    private final String name;


    CatalogEntry( String name ) {
        this.name = name;
    }


    public String getName() {
        return name;
    }


    ;
}
