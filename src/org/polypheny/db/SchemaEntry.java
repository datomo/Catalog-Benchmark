package org.polypheny.db;


public class SchemaEntry extends CatalogEntry {


    public SchemaEntry( String name ) {
        super( name );
    }


    // Could be used directly
    @Override
    public byte[] getBytes() {
        return Serializer.toByteArray( this );
    }


}
