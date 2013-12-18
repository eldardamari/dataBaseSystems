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
            Scanner in = new Scanner(System.in).useDelimiter(";");
            String [] row = in.next().split("\\s");

            String command = row[0];
          

            if (command.equals("create")){
                    createSchema(conn);}

            else if (command.equals("drop")){
                    dropSchema(conn);}
            
            else if (command.equals("load")){
                    String filename = row[1];
                    String tablename = row[2];
                    loadFilesToTabel(filename,tablename,conn);}
            
            else if (command.equals("print")){
                    printTable(row[1],conn);}

            else if (command.equals("sql")){
                    String sqlCommand = "";
                        for(int i=1 ; i < row.length ; i++){
                            if (i==row.length-1)   {sqlCommand +=row[i] + ";";}
                            else        {sqlCommand += row[i] + " ";}
                        }
                    sqlQuery(sqlCommand,conn);}

            else if (command.equals("report")){
                    printReport(row[1],conn);}

            else if (command.equals("query")){
                    printQuery(row[1],conn);}
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

        // persons trigger
        activePersonTrigger(stmt);

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
       
        // relation trigger
        activeRelationTrigger(stmt);

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
        // cars_owned_by_people trigger
        activeDateTrigger(stmt);

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
public static void printTable(String table,Connection conn){

    try {
    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery("SELECT * FROM " + table + ";");
    ResultSetMetaData rsmd = rs.getMetaData();
    int table_number = 0;
    boolean header = false;
    int columnsNumber = rsmd.getColumnCount();

    if (table.equals("cars"))                   { table_number = 1;}
    if (table.equals("persons"))                { table_number = 2;}
    if (table.equals("cars_owned_by_people"))   { table_number = 3;}
    if (table.equals("relations"))              { table_number = 4;}

    while (rs.next()) {

        switch(table_number){
            case 1:
                if(!header) {
                    System.out.format("| %-8s | %-20s | %-15s | %-8s |", 
                            rsmd.getColumnName(1).toUpperCase(),
                            rsmd.getColumnName(2).toUpperCase(),
                            rsmd.getColumnName(3).toUpperCase(),
                            rsmd.getColumnName(4).toUpperCase());

                    System.out.println();//Move to the next line to print the next row.           
                    System.out.println("----------------------------------------"+
                            "------------------------");//Move to the next line to print the next row.           
                    header = true;
                }
                System.out.format("| %8s | %-20s | %-15s | %8s |", rs.getString(1),rs.getString(2),rs.getString(3),rs.getString(4)); //Print one element of a row
                System.out.println();//Move to the next line to print the next row.           
                break;
            case 2:
                if(!header){
                    System.out.format("| %-4s | %-4s | %-16s | %-12s | %-13s | %-21s | %-18s | %-6s | %-12s | %-16s |", 
                            rsmd.getColumnName(1).toUpperCase(),
                            rsmd.getColumnName(2).toUpperCase(),
                            rsmd.getColumnName(3).toUpperCase(),
                            rsmd.getColumnName(4).toUpperCase(),
                            rsmd.getColumnName(5).toUpperCase(),
                            rsmd.getColumnName(6).toUpperCase(),
                            rsmd.getColumnName(7).toUpperCase(),
                            rsmd.getColumnName(8).toUpperCase(),
                            rsmd.getColumnName(9).toUpperCase(),
                            rsmd.getColumnName(10).toUpperCase());

                    System.out.println();//Move to the next line to print the next row.           
                    System.out.println("-------------------------------------------"+
                            "-------------------------------------------");//Move to the next line to print the next row.           
                    header = true;
                }
                System.out.format("| %4s | %4s | %-16s | %-12s | %13s | %-21s | %-18s | %-6s | %12s | %-16s |", 
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getString(5),
                        rs.getString(6),
                        rs.getString(7),
                        rs.getString(8),
                        rs.getString(9),
                        rs.getString(10));
                System.out.println();//Move to the next line to print the next row.           
                break;
            case 3:
                if(!header){
                    System.out.format("| %9s | %6s | %-6s | %-14s |", 
                            rsmd.getColumnName(1).toUpperCase(),
                            rsmd.getColumnName(2).toUpperCase(),
                            rsmd.getColumnName(3).toUpperCase(),
                            rsmd.getColumnName(4).toUpperCase());

                    System.out.println();//Move to the next line to print the next row.           
                    System.out.println("-------------------------------------------");
                    header = true;
                }
                System.out.format("| %9s | %6s | %-6s | %-14s |", 
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4));
                System.out.println();//Move to the next line to print the next row.           
                break;
            case 4:
                if(!header){
                    System.out.format("| %9s | %11s | %-12s |", 
                            rsmd.getColumnName(1).toUpperCase(),
                            rsmd.getColumnName(2).toUpperCase(),
                            rsmd.getColumnName(3).toUpperCase());

                    System.out.println();//Move to the next line to print the next row.           
                    System.out.println("----------------------------------------");
                    header = true;
                }
                System.out.format("| %9s | %11s | %-12s |", 
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3));
                System.out.println();//Move to the next line to print the next row.           
                break;
        }
    }
    } catch (SQLException e){
        System.out.println("In Print Table Exception - " + e.getMessage());}
}

// SQL Query
public static void sqlQuery(String query,Connection conn){
    try {
    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery(query);
    ResultSetMetaData rsmd = rs.getMetaData();
    int table_number = 0;
    boolean header = false;
    int columnsNumber = rsmd.getColumnCount();
    int i,padd = 0;
    String[] paddings = new String[columnsNumber];

    for (i=1,padd = 0; i <=columnsNumber ; i++, padd++){

        int number = new String(rsmd.getColumnName(i)).length() + 5;
        paddings[padd] = new String("|%-"+ new Integer(number).toString()+"s");
        System.out.format(paddings[padd],rsmd.getColumnName(i).toUpperCase());
    }
    System.out.println();

    while (rs.next()) {
        for (i=1,padd=0 ; i <=columnsNumber ; i++,padd++){
            System.out.format(paddings[padd],rs.getString(i));
        }
        System.out.println();
    }
    } catch (SQLException e){
        System.out.println("In SQL QUERY Exception - " + e.getMessage());}
}

// Print Report
public static void printReport(String report,Connection conn){

    String query = "";
    int reportNumber = Integer.parseInt(report);

    switch(reportNumber){

        case 1:
            String CHILDRENS =  "(SELECT id,id_relative,relationship FROM " +
                "(persons NATURAL JOIN relations) WHERE "   +
                "(id=id_person AND relationship='child'))";

            query    =   "SELECT T.id AS parent,S.id,S.age,S.workclASs,"+
                "S.education,S.education_num,S.marital_status,"+
                "S.race,S.sex,S.capital_gain,S.native_country "+
                "FROM " + CHILDRENS +
                "AS T join persons S on (T.id_relative = S.id) "+
                "ORDER BY parent,age desc;";
            break;
        case 2:
            query = "(SELECT * FROM (cars_owned_by_people NATURAL JOIN cars) "+
                    "ORDER BY person_id,date_purchased DESC);";
            break;
        case 3:
            String MOMS_AND_KIDS =  "(SELECT id, COUNT(id_relative) as numOfKids"+
                " FROM persons NATURAL JOIN relations "+
                "WHERE (sex='Female' AND id=id_person AND native_country != 'NULL') GROUP BY id)";

            query = "SELECT native_country ,AVG(numOfKids) FROM " + MOMS_AND_KIDS +
                " as T NATURAL JOIN persons AS S WHERE (T.id=S.id) GROUP BY native_country;";
            break;
    }
            sqlQuery(query,conn);
}

// Print Query
public static void printQuery(String number,Connection conn){
    String query = "";
    int qNum = Integer.parseInt(number);

    switch(qNum){

        case 1:
            query = "(SELECT id FROM persons ORDER BY "+
                                "capital_gain DESC LIMIT 1);";

            break;


        case 2:
            query = "(SELECT id FROM (SELECT id, count(id_relative) AS kids "+
                "FROM persons NATURAL JOIN relations WHERE id = id_person AND "+
                "relationship='child' GROUP BY id ORDER BY kids DESC LIMIT 1) AS T);";

            break;
        case 3:
            String MOMS_AND_KIDS =  "(SELECT id, COUNT(id_relative) as numOfKids"+
                " FROM persons NATURAL JOIN relations "+
                "WHERE (sex='Female' AND id=id_person AND native_country != 'NULL') GROUP BY id)";

            query = "SELECT native_country ,AVG(numOfKids) FROM " + MOMS_AND_KIDS +
                " as T NATURAL JOIN persons AS S WHERE (T.id=S.id) GROUP BY native_country;";
            break;
    }
            printScalar(query,qNum,conn);
}
// Send query to sever and printing scalar
public static void printScalar(String query,int qNum, Connection conn){
    try {
    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery(query);
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnsNumber = rsmd.getColumnCount();

    while(rs.next()){
    if(qNum == 1) {System.out.println("The richest men in db is : " + rs.getString(1));}
    if(qNum == 2) {System.out.println("The person having the max childern : " + rs.getString(1));}
    }

    }
     catch (SQLException e){
        System.out.println("In SQL QUERY Exception - " + e.getMessage());}
}

// Trigger for persons table
public static void activePersonTrigger(Statement stmt) {

    String sql =   "CREATE TRIGGER person_trigger " +
        "BEFORE INSERT ON persons " +
        "FOR EACH ROW " +
        "BEGIN " +
        "IF NEW.race not in ('White', 'Asian-Pac-Islander', 'Amer-Indian-Eskimo', 'Other', 'Black') " +
        "THEN " +
        "CALL Error_Wrong_Race_Name;" +
        " END IF;" +
        "IF NEW.workclass not in ('Private', 'Self-emp-not-inc', 'Self-emp-inc', 'Federal-gov', 'Local-gov', " +
        "'State-gov', 'Without-pay', 'Never-worked') " +
        "THEN " +
        "CALL Error_Wrong_Workclass;" +
        " END IF;" +
        "IF NEW.education not in ('Bachelors', 'Some-college', '11th', 'HS-grad', 'Prof-school', 'Assoc-acdm'," +
        "'Assoc-voc', '9th', '7th-8th', '12th', 'Masters', '1st-4th', '10th', 'Doctorate'," +
        "'5th-6th','Preschool') " +
        "THEN " +
        "CALL Error_Wrong_Education;" +
        " END IF;" +
        "IF NEW.marital_status not in ('Married-civ-spouse','Divorced','Never-married','Separated','Widowed'," +
        " 'Married-spouse-absent','Married-AF-spouse') " +
        "THEN " +
        "CALL Error_Wrong_Martial_Status;" +
        " END IF;" +
        "IF NEW.sex not in ('Male', 'Female') " +
        "THEN " +
        "CALL Error_Wrong_Sex;" +
        " END IF;" +
        "IF NEW.native_country not in ('United-States', 'Cambodia', 'England', 'Puerto-Rico', 'Canada', " +
        "'Germany', 'Outlying-US(Guam-USVI-etc)', 'India', 'Japan', 'Greece', 'South', 'China', 'Cuba', " +
        "'Iran', 'Honduras', 'Philippines', 'Italy', 'Poland', 'Jamaica', 'Vietnam', 'Mexico', 'Portugal', " +
        "'Ireland', 'France', 'Dominican-Republic', 'Laos', 'Ecuador', 'Taiwan', 'Haiti', 'Columbia', " +
        "'Hungary', 'Guatemala', 'Nicaragua', 'Scotland', 'Thailand', 'Yugoslavia', 'El-Salvador', " +
        "'Trinidad&Tobago', 'Peru', 'Hong', 'Holand-Netherlands') " +
        "THEN " +
        "CALL Error_Wrong_Countrey;" +
        " END IF;" +
        "END;";

    try{ stmt.executeUpdate(sql);}
    catch (SQLException e) {
        System.out.println("TRIGGER FOR PERSONS " + e.getMessage());}
}
// Trigger for relations - child cant be older than his parents
public static void activeRelationTrigger(Statement stmt) {

    String sql =   "CREATE TRIGGER relation_trigger " +
        "BEFORE INSERT ON relations " +
        "FOR EACH ROW " +
        "BEGIN " +
        "DECLARE husbandSalary, wifeSalary, personAge, childAge INT; "+
        "IF NEW.relationship NOT IN ('wife', 'husband', 'child') THEN "+
        "CALL Error_Relationship_Name; "                        +
        "END IF;"                                               +
        "IF NEW.relationship in ('child') THEN "                +
        "set @personAge = "                                     +
        "(SELECT (age) FROM persons WHERE id=NEW.id_person); "  +
        "set @childAge = "                                      +
        "(SELECT (age) FROM persons WHERE id=NEW.id_relative); "+
        "IF @personAge < @childAge THEN "                       +
        "CALL Error_Parent_Age_Problem; "                       +
        "END IF; END IF; "                                      +
        "IF NEW.relationship in ('husband') THEN "              +
        "set @husbandSalary = "                                 +
        "(SELECT (capital_gain) FROM persons "                  +
        "WHERE id=NEW.id_relative); "                           +
        "set @husbandSalary = "                                 +
        "(SELECT (capital_gain) FROM persons "                  +
        "WHERE id=NEW.id_person); "                             +
        "END IF; "                                              +
        "IF NEW.relationship in ('wife') THEN "                 +
        "set @husbandSalary = "                                 +
        "(SELECT (capital_gain) FROM persons "                  +
        "WHERE id=NEW.id_person); "                             +
        "set @husbandSalary = "                                 +
        "(SELECT (capital_gain) FROM persons "                  +
        "WHERE id=NEW.id_relative); "                           +
        "END IF; "                                              +
        "IF @husbandSalary > @wifeSalary THEN "                 +
        "CALL Error_Husband_Cant_Earn_More_Than_His_Wife; "     +
        "END IF; "                                              +
        "END;";

    try{ stmt.executeUpdate(sql);}
    catch (SQLException e) {
        System.out.println("TRIGGER FOR relationship -  " + e.getMessage());}
}
// Trigger for Cars Owned By People - Date can't be more than today's date.
public static void activeDateTrigger(Statement stmt) {

    String sql =   "CREATE TRIGGER date_trigger "   +
        "BEFORE INSERT ON cars_owned_by_people "    +
        "FOR EACH ROW "                             +
        "BEGIN "                                    +
        "IF NEW.date_purchased > CURDATE() THEN "   +
        "CALL Error_Date_Is_Not_Vaild; "            +
        "END IF; "                                  +
        "END;";

    try{ stmt.executeUpdate(sql);}
    catch (SQLException e) {
        System.out.println("TRIGGER FOR Cars Owned by People -  " + e.getMessage());}
}
}
