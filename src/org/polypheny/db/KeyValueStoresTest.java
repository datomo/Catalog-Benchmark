package org.polypheny.db;


import org.jetbrains.annotations.NotNull;


public class KeyValueStoresTest {

    static int iter = 1;


    public static void main( String[] args ) {
        useCatalog( new MapDbCatalog() );
        useCatalog( new RocksDbCatalog() );
    }


    private static void useCatalog( DbCatalog catalog ) {

        long startTime = System.nanoTime();
        for ( int i = 0; i < iter; i++ ) {
            fillCatalog( catalog );
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


    private static void fillCatalog( DbCatalog catalog ) {

        catalog.addSchema( new SchemaEntry( "test" ) );
        /*catalog.addTable( new TableEntry( "test", "testTable" ) );
        catalog.addColumn( new ColumnEntry( "test", "testTable", "testColumn" ) );

        catalog.getColumn( "test", "testTable", "testColumn" ).getName();
        catalog.getTable( "test", "testTable" ).getName();
        catalog.getSchema( "test" ).getName();*/

        catalog.getSchemaNames().forEach( System.out::println );

    }


}
