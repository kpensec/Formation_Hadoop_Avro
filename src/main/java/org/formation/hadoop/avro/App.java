package org.formation.hadoop.avro;

import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Hello Avro!!
 *
 */
public class App
{
    public static void main( String[] args )
    {

        User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);

        User user2 = new User("Ben", 7, "red");
        User user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();

        // Serialize on disk
        File file = new File("users.avro");
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        try(DataFileWriter<User> dataFileWriter = new  DataFileWriter<User>(userDatumWriter)) {
            dataFileWriter.create(user1.getSchema(), file);
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
            dataFileWriter.append(user3);
        } catch (IOException e) {
            e.printStackTrace();
        }


        // Deserialize from disk
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        try(DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader)) {
            User user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
                System.out.println(user);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }




        try {
            // Generic reader serialize:
            Schema schema = new Schema.Parser().parse(new File("src/main/avro/user.avsc"));
            GenericRecord guser1 = new GenericData.Record(schema);
            guser1.put("name", "Alyssa");
            guser1.put("favorite_number", 256);

            GenericRecord guser2 = new GenericData.Record(schema);
            guser2.put("name", "Ben");
            guser2.put("favorite_number", 7);
            guser2.put("favorite_color", "blue, oh no red aaaaaahhh...");

            file = new File("users2.avro");
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
            try(DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter)) {
                dataFileWriter.create(schema, file);
                dataFileWriter.append(guser1);
                dataFileWriter.append(guser2);
            }

            // Generic reader deserialize:
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
            try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader)) {
                for (GenericRecord u : dataFileReader) {
                    System.out.println(u);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
