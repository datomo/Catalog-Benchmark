package org.polypheny.db;


import lombok.Getter;


public class ColumnEntry extends CatalogEntry {

    @Getter
    private final String table;
    @Getter
    private final String schema;


    ColumnEntry( String schema, String table, String name ) {
        super( name );
        this.schema = schema;
        this.table = table;
    }


    @Override
    public byte[] serialize() {
        return Serializer.serialize( this );
    }
}
