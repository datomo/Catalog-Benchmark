package catalog.db;


import lombok.Getter;


public class TableEntry extends CatalogEntry {

    @Getter
    final String schema;

    TableEntry( String schema, String name ) {
        super( name );
        this.schema = schema;
    }


    @Override
    byte[] serialize() {
        return Serializer.serialize( this );
    }
}
