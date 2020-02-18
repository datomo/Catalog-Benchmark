package org.polypheny.db;


import lombok.Getter;


public class TableEntry extends CatalogEntry {

    @Getter
    final String schema;

    TableEntry( String schema, String name ) {
        super( name );
        this.schema = schema;
    }


    @Override
    byte[] getBytes() {
        return Serializer.toByteArray( this );
    }
}
