package catalog.db.main;


import catalog.db.main.entity.ColumnEntry;
import catalog.db.main.entity.SchemaEntry;
import catalog.db.main.entity.TableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;


/**
 * Should allow some broad benchmarking of different catalog key-value implementations
 */
public class KeyValueStores {

    static int iter = 1;


    public static void main( String[] args ) {

        List<DbCatalog> catalogs = new ArrayList<>();
        catalogs.add( new MapDbCatalog() );
        catalogs.add( new RocksDbCatalog() );

        catalogs.forEach(  KeyValueStores::fill );

        timeMultipleCatalogs( catalogs, DbCatalog::getColumns, "GetColumns()" );
        timeMultipleCatalogs( catalogs, DbCatalog::getColumnNames, "GetColumnNames()" );

        catalogs.forEach( DbCatalog::close );


    }


    /**
     * Simple wrapper for applying a timeing function to multiple catalog implementations
     */
    private static void timeMultipleCatalogs( List<DbCatalog> catalogs, Consumer<DbCatalog> func, String functName ) {
        catalogs.forEach( c -> {
            timeCatalog( c, func, functName );
        } );
    }


    /**
     * Repeatedly executes a function on a catalog, times it and outputs it in the end
     * @param catalog the catalog on which the function gets executed
     * @param func function which gets executed on the catalog
     * @param functName name of the function
     */
    private static void timeCatalog( DbCatalog catalog, Consumer<DbCatalog> func, String functName ) {

        long startTime = System.nanoTime();
        for ( int i = 0; i < iter; i++ ) {
            func.accept( catalog );
        }

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);

        printDuration( catalog.getClass().getSimpleName(), functName, duration );
    }


    /**
     * "Pretty prints" the duration results to the terminal
     */
    private static void printDuration( String className, String functName, long duration ) {
        System.out.println( "\n" + className + ": " + functName );
        System.out.println( "Overall: " + insertQuotes( duration ) + "ns" );
        System.out.println( "Single: " + insertQuotes( duration / iter ) + "ns" );
    }


    /**
     * Helper function to insert ' into number for better readability
     * @return the duration separated by '
     */
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


    /**
     * Simple test fill and execute method for testing
     * @param catalog the catalog which gets used
     */
    private static void fillTest( DbCatalog catalog ) {

        catalog.addSchema( new SchemaEntry( 1, "schema", 1, "APP", 1, "user" ));
        catalog.addTable( new TableEntry( 1, "table", 1, "schema", 1, "APP", 1, "user", "???", (long) 10000 ) );
        catalog.addColumn( new ColumnEntry( 1, "column", 1, "table", 1, "schema", 1, "APP", 1, 255, 2, false ) );

        catalog.getColumn( "test", "testTable", "testColumn" );
        catalog.getTable( "test", "testTable" );
        catalog.getSchema( "test" );

        catalog.getSchemaNames();
        catalog.getSchemaNames();
        catalog.getSchemas();

    }


    /**
     * Fills the catalog with some dummy data
     * @param catalog catalog which is operated on
     */
    private static void fill( DbCatalog catalog ) {

        catalog.addSchema( new SchemaEntry( 1, "schema", 1, "APP", 1, "user" ));
        catalog.addSchema( new SchemaEntry( 1, "schema3", 1, "APP", 1, "user" ));
        catalog.addTable( new TableEntry( 1, "table", 1, "schema", 1, "APP", 1, "user", "???", (long) 10000 ) );
        catalog.addColumn( new ColumnEntry( 1, "column", 1, "table", 1, "schema", 1, "APP", 1, 255, 2, false ) );
        catalog.addSchema( new SchemaEntry( 1, "schema2", 1, "APP", 1, "user" ));
        catalog.addTable( new TableEntry( 1, "table2", 1, "schema", 1, "APP", 1, "user", "???", (long) 10000 ) );
        catalog.addColumn( new ColumnEntry( 1, "column2", 1, "table2", 1, "schema", 1, "APP", 1, 255, 2, false ) );

    }


}
