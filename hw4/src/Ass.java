import java.sql.*;
import java.io.Console;
import java.util.Scanner;

public class Ass {
// JDBC driver name and database URL
static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
static final String DB_URL = "jdbc:mysql://localhost:1234/dbs141_user15";

//  Database credentials
static final String USER = "dbs141_user15";
static final String PASS = "dbsdbs";


public static void main(String[] args) {
    Connection conn = null;
    Statement stmt  = null;
    System.out.println("Hello!");

    try{
        //STEP 2: Register JDBC driver
        Class.forName("com.mysql.jdbc.Driver");

        //STEP 3: Open a connection
        System.out.println("Connecting to a selected database...");
        conn = DriverManager.getConnection(DB_URL, USER, PASS);
        System.out.println("Connected database successfully...");

        //STEP 4: Execute a query
        System.out.println("Creating table in given database...");

        // Infinte loop representing command line
        while(true){

            System.out.print("> ");
            Scanner in = new Scanner(System.in).useDelimiter("\\s");
          
            String command = in.next();

            if (command.equals("create")){
                    createSchema(conn);}

            else if (command.equals("drop")){
                    dropSchema(conn);}
            
            else if (command.equals("load")){
                    String filename = in.next();
                    String tablename = in.next();
                    loadFilesToTabel(filename,tablename,conn);}
            
            else if (command.equals("print")){
                    printTable(in.next());}

            else if (command.equals("sql")){
                    sqlQuery(in.next());}

            else if (command.equals("report")){
                    printReport(in.next());}

            else if (command.equals("query")){
                    printQuery(in.next());}
            else {
                System.out.println("Error: Bad input, please try again");}
            
            in.reset();
            }
        }

    catch(SQLException se){
        //Handle errors for JDBC
        se.printStackTrace();
    }catch(Exception e){
        //Handle errors for Class.forName
        e.printStackTrace();
    }finally{
        //finally block used to close resources
        try{
            if(stmt!=null)
                conn.close();
        }catch(SQLException se){
        }// do nothing
        try{
            if(conn!=null)
                conn.close();
        }catch(SQLException se){
            se.printStackTrace();
        }//end finally try
    }//end try

    System.out.println("Goodbye!");
}//end main

// Create Schema
public static void createSchema(Connection conn){
    System.out.println("in create schema");
    /*Connection conn = null;*/
    Statement  stmt = null;
 try{   
    stmt = conn.createStatement();

        String sql = "CREATE TABLE persons" +
            " (id INT NOT NULL PRIMARY KEY,"+
            " age INT,"                     + 
            " workclass VARCHAR(255),"      + 
            " education VARCHAR(255),"      + 
            " education_num INT,"           + 
            " marital_status VARCHAR(255)," + 
            " race VARCHAR(255),"           + 
            " sex VARCHAR(255),"            + 
            " capital_gain INT,"            + 
            " native_country VARCHAR(255));"; 

        stmt.executeUpdate(sql);
        System.out.println("Created persons table sucssfully..");
        
        sql = "CREATE TABLE relations" +
            " (id_person INT NOT NULL,"              +
            " id_relative INT NOT NULL,"             + 
            " relationship VARCHAR(255),"            +
            " PRIMARY KEY (id_person, id_relative)," + 
            " FOREIGN KEY (id_person)"               +
            " REFERENCES persons(id),"               +
            " FOREIGN KEY (id_relative)"             +
            " REFERENCES persons(id));";

        stmt.executeUpdate(sql);

        System.out.println("Created Married and Descendants table sucssfully..");
        
        sql = "CREATE TABLE cars"                   +
            " (car_id INT NOT NULL PRIMARY KEY,"    +
            " car_manufacturer VARCHAR(255),"       + 
            " car_model VARCHAR(255),"              +
            " car_year INT);";

        stmt.executeUpdate(sql);
        System.out.println("Created Cars table sucssfully..");
        
        sql = "CREATE TABLE cars_owned_by_people"   +
            " (person_id INT NOT NULL,"             +
            " car_id INT NOT NULL,"                 +
            " color VARCHAR(255),"                  + 
            " date_purchased DATE,"                 +
            " PRIMARY KEY (person_id, car_id),"     +
            " FOREIGN KEY (person_id)"              +
            " REFERENCES persons(id),"              +
            " FOREIGN KEY (car_id)"                 +
            " REFERENCES cars(car_id));";        

        stmt.executeUpdate(sql);
        System.out.println("Created Cars Owned by People table sucssfully..");
 
 }catch(SQLException se){
     //Handle errors for JDBC
     se.printStackTrace();
 }catch(Exception e){
     //Handle errors for Class.forName
     e.printStackTrace();}
 /*}finally{
     //finally block used to close resources
     try{
         if(persons!=null)
             conn.close();
     }catch(SQLException se){
     }// do nothing
     try{
         if(conn!=null)
             conn.close();
     }catch(SQLException se){
         se.printStackTrace();
     }//end finally try
 }//end try*/
}

// Drop Schema
public static void dropSchema(Connection conn){
    System.out.println("in drop");
    Statement  stmt = null;

    try{
        stmt = conn.createStatement();

        String sql =" DROP TABLE IF EXISTS"     +
                    " relations," +
                    " cars_owned_by_people, cars, persons;";
        stmt.executeUpdate(sql);

    }catch(SQLException se){
        //Handle errors for JDBC
        se.printStackTrace();
    }catch(Exception e){
        //Handle errors for Class.forName
        e.printStackTrace();}

}
// Load a file to specific table
public static void loadFilesToTabel(String file,String table,Connection conn){
    System.out.println("in load with arguments:");
    ReadCVS.parseFile(file,table,conn);
}

// Print specific table
public static void printTable(String table){
    System.out.println("in print tablw");
}

// SQL Query
public static void sqlQuery(String query){
    System.out.println("in sql query");
}

// Print Report
public static void printReport(String report){
    System.out.println("in print report");
}

// Print Query
public static void printQuery(String query){
    System.out.println("in print query");
}

}

/*sql =   "CREATE TRIGGER Race " +
"BEFORE INSERT ON Persons " +
"FOR EACH ROW " +
"BEGIN " +
"IF NEW.Race not in ('White', 'Asian-Pac-Islander', 'Amer-Indian-Eskimo', 'Other', 'Black') " +
"THEN " +
"CALL wrong_race;" +
" END IF;" +
"IF NEW.Workclass not in ('Private', 'Self-emp-not-inc', 'Self-emc-inc', 'Federal-gov', 'Local-gov', " +
"'State-gov', 'Without-pay', 'Never-worked') " +
"THEN " +
"CALL wrong_Workclass;" +
" END IF;" +
"IF NEW.Education not in ('Bachelors', 'Some-college', '11th', 'HS-grad', 'Prof-school', 'Assoc-acdm'" +
"'Assoc-voc', '9th', '7th-8th', '12th', 'Masters', '1st-4th', '10th', 'Doctorate'," +
"'5th-6th','Preschool') " +
"THEN " +
"CALL wrong_Education;" +
" END IF;" +
"IF NEW.Marital_status not in ('Married-civ-spouse','Divorced','Never-married','Seperated','Widowed'," +
" 'Married-spouse-absent','Married-AF-spouse') " +
"THEN " +
"CALL wrong_Martial_Status;" +
" END IF;" +
"IF NEW.Sex not in ('Male', 'Female') " +
"THEN " +
"CALL wrong_Sex;" +
" END IF;" +
"IF NEW.Native_country not in ('United-States', 'Cambodia', 'England', 'Puerto-Rico', 'Canada', " +
"'Germany', 'Outlying-US(Guam-USVI-etc)', 'India', 'Japan', 'Greece', 'South', 'China', 'Cuba', " +
"'Iran', 'Honduras', 'Philippines', 'Italy', 'Poland', 'Jamaica', 'Vietnam', 'Mexico', 'Portugal', " +
"'Ireland', 'France', 'Dominican-Republic', 'Laos', 'Ecuador', 'Taiwan', 'Haiti', 'Columbia', " +
"'Hungary', 'Guatemala', 'Nicaragua', 'Scotland', 'Thailand', 'Yugoslavia', 'El-Salvador', " +
"'Trinidad&Tobago', 'Peru', 'Hong', 'Holand-Netherlands') " +
"THEN " +
"CALL wrong_Countrey;" +
" END IF;" +
"END;";*/
