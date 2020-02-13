package org.polypheny.db;


import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;


/**
 * Serializable collection of classes
 */
public class DbSerialize {

    static class SchemaSerializer implements Serializer<SchemaEntry>, Serializable {

        @Override
        public void serialize( @NotNull DataOutput2 dataOutput2, @NotNull SchemaEntry schema ) throws IOException {
            dataOutput2.writeUTF( schema.getName() );
            TableListSerializer serializer = new TableListSerializer();
            serializer.serialize( dataOutput2, schema.getTables() );

        }


        @Override
        public SchemaEntry deserialize( @NotNull DataInput2 dataInput2, int i ) throws IOException {
            String name = dataInput2.readUTF();
            TableListSerializer serializer = new TableListSerializer();
            List<TableEntry> tables = new ArrayList( serializer.deserialize( dataInput2, i ) );
            return new SchemaEntry( name, tables );
        }
    }


    /**
     * TODO: list list list why
     */
    static class TableListSerializer implements Serializer<List<TableEntry>>, Serializable {

        @Override
        public void serialize( @NotNull DataOutput2 dataOutput2, @NotNull List<TableEntry> list ) throws IOException {
            dataOutput2.writeInt( list.size() );
            TableSerializer serializer = new TableSerializer();
            list.forEach( e -> {
                try {
                    serializer.serialize( dataOutput2, e );
                } catch ( IOException ex ) {
                    ex.printStackTrace();
                }
            } );
        }


        @Override
        public List<TableEntry> deserialize( @NotNull DataInput2 dataInput2, int i ) throws IOException {
            int size = dataInput2.readInt();
            List<TableEntry> tables = new ArrayList<>();
            TableSerializer serializer = new TableSerializer();
            for ( int j = 0; j < size; j++ ) {
                tables.add( serializer.deserialize( dataInput2, i ) );
            }
            return tables;
        }
    }


    static class TableSerializer implements Serializer<TableEntry>, Serializable {

        @Override
        public void serialize( @NotNull DataOutput2 dataOutput2, @NotNull TableEntry table ) throws IOException {
            dataOutput2.writeUTF( table.getName() );
        }


        @Override
        public TableEntry deserialize( @NotNull DataInput2 dataInput2, int i ) throws IOException {
            return new TableEntry( dataInput2.readUTF() );
        }
    }


    static class ColumnSerializer implements Serializer<ColumnEntry>, Serializable {

        @Override
        public void serialize( @NotNull DataOutput2 dataOutput2, @NotNull ColumnEntry column ) throws IOException {
            dataOutput2.writeUTF( column.getName() );
        }


        @Override
        public ColumnEntry deserialize( @NotNull DataInput2 dataInput2, int i ) throws IOException {
            return new ColumnEntry( dataInput2.readUTF() );
        }
    }
}
