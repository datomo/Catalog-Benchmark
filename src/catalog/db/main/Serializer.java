package catalog.db.main;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;


public class Serializer {


    public static <T extends Serializable> byte[] serialize( T entry ) {
        // https://stackoverflow.com/questions/2836646/java-serializable-object-to-byte-array
        byte[] bytes = null;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream( bos );
            out.writeObject( entry );
            out.flush();
            bytes = bos.toByteArray();
            bos.close();
            out.close();
            return bytes;
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        return bytes;

    }


    // caller needs to handle casting for now
    public static <S extends Serializable> S deserialize( byte[] bytes ) {
        S object = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream( bytes );
            ObjectInput in = new ObjectInputStream( bis );
            object = (S) in.readObject();
            bis.close();
            in.close();
            return object;
        } catch ( IOException | ClassNotFoundException e ) {
            e.printStackTrace();
        }
        return object;

    }

}
