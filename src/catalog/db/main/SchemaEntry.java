package catalog.db.main;


public class SchemaEntry extends CatalogEntry {


    public SchemaEntry( String name ) {
        super( name );
    }


    @Override
    byte[] serialize() {
        return Serializer.serialize( this );
    }


    @Override
    public boolean equals( Object o ) {
        if ( !(o instanceof SchemaEntry) ) {
            return false;
        }
        SchemaEntry other = (SchemaEntry) o;
        return this.getName().equals( other.getName() );
    }
}
