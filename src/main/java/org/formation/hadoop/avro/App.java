package org.formation.hadoop.avro;

import org.formation.hadoop.avro.example.avro.User;

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
       File file = new File("users.avro");
       DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
       try(DataFileWriter<User> dataFileWriter = new  DataFileWriter<User>(userDatumWriter)) {
         dataFileWriter.create(user1.getSchema(), file);
         dataFileWriter.append(user1);
         dataFileWriter.append(user2);
         dataFileWriter.append(user3);
       }
        
    }
}
