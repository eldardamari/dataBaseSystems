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
                    dropSchema();}
            
            else if (command.equals("load")){
                    String filename = in.next();
                    String tablename = in.next();
                    loadFilesToTabel(filename,tablename);}
            
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
    Statement  persons = null;
 try{   
    persons = conn.createStatement();

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

        persons.executeUpdate(sql);
        System.out.println("Created persons table sucssfully..");
        
        sql = "CREATE TABLE married_and_descendants" +
            " (id_person INT NOT NULL,"              +
            " id_relative INT NOT NULL,"             + 
            " relationship VARCHAR(255),"            +
            " PRIMARY KEY (id_person, id_relative)," + 
            " FOREIGN KEY (id_person)"               +
            " REFERENCES persons(id),"               +
            " FOREIGN KEY (id_relative)"             +
            " REFERENCES persons(id));";

        persons.executeUpdate(sql);
        System.out.println("Created Married and Descendants table sucssfully..");
        
        sql = "CREATE TABLE cars"                   +
            " (car_id INT NOT NULL PRIMARY KEY,"    +
            " car_manufacturer VARCHAR(255),"       + 
            " car_model VARCHAR(255),"              +
            " car_year INT);";

        persons.executeUpdate(sql);
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

        persons.executeUpdate(sql);
        System.out.println("Created Cars Owned by People table sucssfully..");
 
 }catch(SQLException se){
     //Handle errors for JDBC
     se.printStackTrace();
 }catch(Exception e){
     //Handle errors for Class.forName
     e.printStackTrace();
 }finally{
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
 }//end try
}

// Drop Schema
public static void dropSchema(){
    System.out.println("in drop");
}

// Load a file to specific table
public static void loadFilesToTabel(String file,String table){
    System.out.println("in load with arguments:");
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
