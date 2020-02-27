package catalog.db.test;


import static org.junit.Assert.assertTrue;

import catalog.db.main.MapDbCatalog;
import catalog.db.main.Pattern;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class MapDBTest {

    MapDbCatalog catalog;


    @Before
    public void initDatabase() {
        catalog = new MapDbCatalog( "mapDB-singleTests" );
        catalog.clear();
    }


    @Test
    public void testStringArrayKey() {
        catalog.addColumn( "schema", "table", "column", 3L );
        assertTrue( catalog.getColumn( new Pattern( "schema" ), new Pattern( "table" ), new Pattern( "column" ) ).contains( 3L ) );

        catalog.addColumn( "schema", "table", "column2", 2L );
        List<Long> list =  catalog.getColumn( new Pattern( "schema" ), new Pattern( "table" ), null);
        assertTrue( list.contains( 2L ) && list.contains( 3L ) && list.size() == 2 );

    }


    @After
    public void close() {
        catalog.close();
    }

}
