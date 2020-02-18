package org.polypheny.db;


import java.util.function.Consumer;
import org.jetbrains.annotations.NotNull;


public class KeyValueStoresTest {

    static int iter = 1000000;


    public static void main( String[] args ) {
        timeCatalog( new MapDbCatalog(), KeyValueStoresTest::fillTest );
        timeCatalog( new RocksDbCatalog(), KeyValueStoresTest::fillTest );
        DbCatalog mapDB = new MapDbCatalog();
        DbCatalog rocksDB = new RocksDbCatalog();

        fill( mapDB );
        fill( rocksDB );

        timeCatalog( mapDB, DbCatalog::getColumns );
        timeCatalog( rocksDB, DbCatalog::getColumns );
    }


    private static void timeCatalog( DbCatalog catalog, Consumer<DbCatalog> func ) {

        long startTime = System.nanoTime();
        for ( int i = 0; i < iter; i++ ) {
            func.accept( catalog );
        }
        catalog.close();
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);

        logDuration( catalog.getClass().getSimpleName(), duration );
    }


    private static void logDuration( String className, long duration ) {
        System.out.println( "\n" + className );
        System.out.println( "Overall: " + insertQuotes( duration ) + "ns" );
        System.out.println( "Single: " + insertQuotes( duration / iter ) + "ns" );
    }


    @NotNull
    private static StringBuilder insertQuotes( long duration ) {
        StringBuilder result = new StringBuilder();
        int i = Long.toString( duration ).length() - 1;
        for ( char c : Long.toString( duration ).toCharArray() ) {
            result.append( c );
            if ( i % 3 == 0 && i != 0 ) {
                result.append( "'" );
            }
            i--;
        }
        return result;
    }


    private static void fillTest( DbCatalog catalog ) {

        catalog.addSchema( new SchemaEntry( "test" ) );
        catalog.addTable( new TableEntry( "test", "testTable" ) );
        catalog.addColumn( new ColumnEntry( "test", "testTable", "testColumn" ) );

        catalog.getColumn( "test", "testTable", "testColumn" ).getName();
        catalog.getTable( "test", "testTable" ).getName();
        catalog.getSchema( "test" ).getName();

        catalog.getSchemaNames();
        catalog.getSchemaNames();
        catalog.getSchemas();

    }


    private static void fill( DbCatalog catalog ) {
        catalog.addSchema( new SchemaEntry( "test" ) );
        catalog.addSchema( new SchemaEntry( "test2" ) );
        catalog.addTable( new TableEntry( "test", "testTable" ) );
        catalog.addTable( new TableEntry( "test", "testTable1" ) );
        catalog.addTable( new TableEntry( "test", "testTable3" ) );
        catalog.addColumn( new ColumnEntry( "test", "testTable", "testColumn" ) );
        catalog.addColumn( new ColumnEntry( "test", "testTable", "testColumn2" ) );
        catalog.addColumn( new ColumnEntry( "test", "testTable", "testColumn3" ) );
    }


}
