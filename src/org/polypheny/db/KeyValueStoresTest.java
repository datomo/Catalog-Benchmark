package org.polypheny.db;


import com.google.common.collect.ImmutableList;
import java.util.concurrent.ConcurrentMap;
import org.mapdb.DB;
import org.mapdb.Serializer;
import org.polypheny.db.DbSerialize.SchemaSerializer;
import org.polypheny.db.MapDbCatalog.TableExistsException;


public class KeyValueStoresTest {


    public static void main( String[] args ) {
        //MapDbCatalog catalog = new MapDbCatalog();
        RocksDbCatalog catalog = new RocksDbCatalog();

        useCatalog( catalog );

        catalog.close();
    }


    private static void useCatalog( DbCatalog catalog ) {

        long startTime = System.nanoTime();
        for ( int i = 0; i < 1000; i++ ) {
            fillCatalog( catalog );
        }
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        System.out.println( duration );
    }


    private static void fillCatalog( DbCatalog catalog ) {

        catalog.addSchema( new SchemaEntry( "test" ) );
        catalog.addTable( new TableEntry( "test", "testTable" ) );
        catalog.addColumn( new ColumnEntry( "test", "testTable", "testColumn" ) );

        catalog.getColumn( "test", "testTable", "testColumn" ).getName();

    }


}
