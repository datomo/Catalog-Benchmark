package catalog.db.main.entity;


import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;


/**
 * Collection of different serializers
 */
public class DbSerialize {

    public static class GenericSerializer<T extends Serializable> implements Serializer<T>, Serializable {


        @Override
        public void serialize( @NotNull DataOutput2 dataOutput2, @NotNull T entry ) throws IOException {
            Serializer.BYTE_ARRAY.serialize( dataOutput2, catalog.db.main.entity.Serializer.serialize( entry ) );

        }


        @Override
        public T deserialize( @NotNull DataInput2 dataInput2, int i ) throws IOException {
            return catalog.db.main.entity.Serializer.deserialize( Serializer.BYTE_ARRAY.deserialize( dataInput2, i ) );
        }
    }


    /**
     * TODO: list list list why
     */
    public static class ListSerializer<T> implements Serializer<ImmutableList<ArrayList<T>>>, Serializable {

        final public Serializer<T> serializer;


        public ListSerializer( @NotNull Serializer<T> serializer ) {
            this.serializer = serializer;
        }


        @Override
        public void serialize( @NotNull DataOutput2 dataOutput2, @NotNull ImmutableList<ArrayList<T>> list ) throws IOException {
            dataOutput2.writeInt( list.size() );

            list.get( 0 ).forEach( e -> {
                try {
                    this.serializer.serialize( dataOutput2, e );
                } catch ( IOException ex ) {
                    ex.printStackTrace();
                }
            } );
        }


        @Override
        public ImmutableList<ArrayList<T>> deserialize( @NotNull DataInput2 dataInput2, int i ) throws IOException {
            int size = dataInput2.readInt();
            ArrayList<T> tables = new ArrayList<>();
            for ( int j = 0; j < size; j++ ) {
                tables.add( serializer.deserialize( dataInput2, i ) );
            }
            return ImmutableList.of( tables );
        }


        @Override
        public boolean equals( Object obj ) {
            return false;
        }
    }



    /*static class TableSerializer implements Serializer<TableEntry>, Serializable {

        @Override
        public void serialize( @NotNull DataOutput2 dataOutput2, @NotNull TableEntry table ) throws IOException {
            dataOutput2.writeUTF( table.getSchema() );
            dataOutput2.writeUTF( table.getName() );
        }


        @Override
        public TableEntry deserialize( @NotNull DataInput2 dataInput2, int i ) throws IOException {
            return new TableEntry( dataInput2.readUTF(), dataInput2.readUTF() );
        }
    }


    static class ColumnSerializer implements Serializer<ColumnEntry>, Serializable {

        @Override
        public void serialize( @NotNull DataOutput2 dataOutput2, @NotNull ColumnEntry column ) throws IOException {
            dataOutput2.writeUTF( column.getSchema() );
            dataOutput2.writeUTF( column.getTable() );
            dataOutput2.writeUTF( column.getName() );
        }


        @Override
        public ColumnEntry deserialize( @NotNull DataInput2 dataInput2, int i ) throws IOException {
            return new ColumnEntry( dataInput2.readUTF(), dataInput2.readUTF(), dataInput2.readUTF() );
        }
    }*/
}
