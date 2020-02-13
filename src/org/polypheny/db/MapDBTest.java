package org.polypheny.db;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.polypheny.db.DbSerialize.ColumnSerializer;
import org.polypheny.db.DbSerialize.SchemaSerializer;
import org.polypheny.db.DbSerialize.TableListSerializer;


public class MapDBTest {

    private static ConcurrentMap<String, ColumnEntry> columns;


    public static void main( String[] args ) {
        DB db = DBMaker
                .memoryDB()
                .make();
        ConcurrentMap<String, String> map = db
                .hashMap( "map", Serializer.STRING, Serializer.STRING )
                .createOrOpen();
        map.put( "something", "hai" );

        map.forEach( ( k, v ) -> System.out.println( k + ": " + v ) );

        makeSchemas( db );

        // testTableEntryList( db );

        db.close();

    }


    private static void testTableEntryList( DB db ) {
        ConcurrentMap<String, List<TableEntry>> tables = db.hashMap( "schemas" )
                .keySerializer( Serializer.STRING )
                .valueSerializer( new TableListSerializer() )
                .createOrOpen();
        List<TableEntry> list = new ArrayList( );
        list.add( new TableEntry( "test1" ) );
        list.add( new TableEntry( "test2" ));
        tables.put( "test", list );

        tables.get( "test" ).forEach( System.out::println );
    }


    private static void makeSchemas( DB db ) {
        ConcurrentMap<String, SchemaEntry> schemas = db.hashMap( "schemas" )
                .keySerializer( Serializer.STRING )
                .valueSerializer( new SchemaSerializer() )
                .createOrOpen();
        ImmutableList<TableEntry> tables = ImmutableList.of( new TableEntry( "name1" ), new TableEntry( "name2" ) );
        schemas.put( "test", new SchemaEntry( "id", tables ) );
        schemas.get( "test" ).getTables().forEach( t -> System.out.println( t.getName() ) );
    }


    private static void makeColumns( DB db ) {
        columns = db.hashMap( "Columns" )
                .keySerializer( Serializer.STRING )
                .valueSerializer( new ColumnSerializer() )
                .createOrOpen();

        columns.put( "test", new ColumnEntry( "id" ) );
        System.out.println( columns.get( "test" ).getName() );
    }


}
