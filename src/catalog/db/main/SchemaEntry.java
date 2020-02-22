package catalog.db.main;


public class SchemaEntry extends CatalogEntry {


    public SchemaEntry( String name ) {
        super( name );
    }


    @Override
    byte[] serialize() {
        return Serializer.serialize( this );
    }


}
