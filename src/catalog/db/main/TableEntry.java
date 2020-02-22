package catalog.db.main;


import lombok.Getter;


public class TableEntry extends CatalogEntry {

    @Getter
    final String schema;

    public TableEntry( String schema, String name ) {
        super( name );
        this.schema = schema;
    }


    @Override
    byte[] serialize() {
        return Serializer.serialize( this );
    }

    @Override
    public boolean equals( Object o ) {
        if (!(o instanceof TableEntry)) {
            return false;
        }
        TableEntry other = (TableEntry) o;
        return this.schema.equals( other.schema ) && this.getName().equals( other.getName() );
    }
}
