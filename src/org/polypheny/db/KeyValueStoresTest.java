package org.polypheny.db;


import com.google.common.collect.ImmutableList;
import java.util.concurrent.ConcurrentMap;
import org.mapdb.DB;
import org.mapdb.Serializer;
import org.polypheny.db.DbSerialize.SchemaSerializer;
import org.polypheny.db.MapDbCatalog.TableExistsException;


public class KeyValueStoresTest {


    public static void main( String[] args ) {

        MapDbCatalog catalog = new MapDbCatalog();

        long startTime = System.nanoTime();
        for(int i = 0; i < 1000; i++){
            fillCatalog( catalog );
        }

        long endTime = System.nanoTime();

        long duration = (endTime - startTime);
        System.out.println( duration );
    }


    private static void fillCatalog( MapDbCatalog catalog ) {
        try {
            catalog.addSchema( "test" );
            catalog.addTable( "test", "testTable" );
            catalog.addColumn( "test", "testTable", "testColumn" );

            catalog.getColumn( "test", "testTable", "testColumn" ).getName();
        } catch ( TableExistsException e ) {
            e.printStackTrace();
        }
    }


    private static void makeSchemas( DB db ) {
        ConcurrentMap<String, SchemaEntry> schemas = db.hashMap( "schemas" )
                .keySerializer( Serializer.STRING )
                .valueSerializer( new SchemaSerializer() )
                .createOrOpen();
        ImmutableList<TableEntry> tables = ImmutableList.of( new TableEntry( "name1" ), new TableEntry( "name2" ) );
        schemas.put( "test", new SchemaEntry( "id" ) );

    }


}
