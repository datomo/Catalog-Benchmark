package catalog.db.test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import catalog.db.main.entity.ColumnEntry;
import catalog.db.main.DbCatalog;
import catalog.db.main.MapDbCatalog;
import catalog.db.main.RocksDbCatalog;
import catalog.db.main.entity.SchemaEntry;
import catalog.db.main.entity.TableEntry;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class CatalogTest {

    private DbCatalog subject;

    @Parameter
    public Class<? extends DbCatalog> aClass;


    @Parameters(name = "{index}: Implementation Class: {0}")
    public static Collection<Object[]> classes() {
        List<Object[]> impls = new ArrayList<>();
        impls.add( new Object[]{ RocksDbCatalog.class } );
        impls.add( new Object[]{ MapDbCatalog.class } );

        return impls;
    }


    @Before
    public void setup() {
        subject = getNewSubject();
        assert subject != null;
        subject.clear();
    }


    private DbCatalog getNewSubject() {
        try {
            return aClass.getDeclaredConstructor( String.class ).newInstance( "testDB-" + aClass.getSimpleName() );
        } catch ( InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Test
    public void testObjectEntries() {
        SchemaEntry schema = new SchemaEntry( 1, "schema", 1, "APP", 1, "user" );
        TableEntry table = new TableEntry( 1, "table", 1, schema.name, schema.id, "APP", 1, "user", "???", (long) 10000 );
        ColumnEntry column = new ColumnEntry( 1, "column", 1, table.name, table.id, schema.name, 1, "APP", 1, 255, 2, false );

        subject.addSchema( schema );
        subject.addTable( table );
        subject.addColumn( column );

        assertEquals( subject.getSchema( "schema" ), schema );
        assertEquals( subject.getTable( "schema", "table" ), table );
        assertEquals( subject.getColumn( "schema", "table", "column" ), column );
    }


    @Test
    public void testObjectNames() {
        setupSimpleEntries();

        assertTrue( subject.getSchemaNames().contains( "schema" ) );
        assertTrue( subject.getTableNames().contains( "schema.table" ) );
        assertTrue( subject.getColumnNames().contains( "schema.table.column" ) );
    }


    @Test
    public void testCleanShutdownPersistence() {
        setupSimpleEntries();
        subject.close();
        DbCatalog reopenedDB = getNewSubject();
        assert reopenedDB != null;

        assertTrue( reopenedDB.getSchemaNames().contains( "schema" ) );
        assertTrue( reopenedDB.getTableNames().contains( "schema.table" ) );
        assertTrue( reopenedDB.getColumnNames().contains( "schema.table.column" ) );

        reopenedDB.close();

    }


    @Test
    public void testMultipleOpen() {
        setupSimpleEntries();

        DbCatalog reopenedDB = getNewSubject();
        assert reopenedDB != null;

        assertTrue( reopenedDB.getSchemaNames().contains( "schema" ) );
        assertTrue( reopenedDB.getTableNames().contains( "schema.table" ) );
        assertTrue( reopenedDB.getColumnNames().contains( "schema.table.column" ) );

        reopenedDB.close();
    }


    private void setupSimpleEntries() {
        SchemaEntry schema = new SchemaEntry( 1, "schema", 1, "APP", 1, "user" );
        TableEntry table = new TableEntry( 1, "table", 1, schema.name, schema.id, "APP", 1, "user", "???", (long) 10000 );
        ColumnEntry column = new ColumnEntry( 1, "column", 1, table.name, table.id, schema.name, 1, "APP", 1, 255, 2, false );

        subject.addSchema( schema );
        subject.addTable( table );
        subject.addColumn( column );
    }


    @After
    public void closeSubject() {
        if ( !subject.isClosed() ) {
            subject.close();
        }

    }


}
