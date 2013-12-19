import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Types;

public class ReadCVS {

    public static void parseFile(String fileName,String table,Connection conn) {

        ReadCVS obj = new ReadCVS();

        String csvFile = "./" + fileName;
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            PreparedStatement pStmnt = null;
            br = new BufferedReader(new FileReader(csvFile));

            if (csvFile.equals("./ex4.data.cars.csv") &&
                    table.equals("cars")) {

                // UPDATING CARS TABLES!!
                insertCars(pStmnt,br,conn,table,cvsSplitBy);

            } else if (csvFile.equals("./ex4.data.persons.csv") &&
                    table.equals("persons")) {

                // UPDATING PERSONS TABLES!!
                insertPersons(pStmnt,br,conn,table,cvsSplitBy);

            } else if (csvFile.equals("./ex4.data.relations.csv") &&
                    table.equals("relations")) {

                // UPDATING REALTIONS TABLES!!
                insertRelations(pStmnt,br,conn,table,cvsSplitBy);

            } else if (csvFile.equals("./ex4.data.car.owned.by.people.csv") &&
                    table.equals("cars_owned_by_people")) {

                // UPDATING CARS OWNED BY PEOPLE TABLES!!
                insertCarsOwnedByPeople(pStmnt,br,conn,table,cvsSplitBy);
                    }

        } catch (SQLException e) {
            System.out.println("In Parsing files(sql) - " + e.getMessage());
        }catch (FileNotFoundException e) {
            System.out.println("In Parsing files(file not found) - " + e.getMessage());
        } catch (IOException e) {
            System.out.println("In Parsing files(IO) - " + e.getMessage());

        }
    }

    public static void insertCars( PreparedStatement pStmnt,
            BufferedReader br,
            Connection conn,
            String table,
            String cvsSplitBy) throws SQLException, IOException{

        String insertCarsTable = "INSERT INTO " + table +
            " (car_id, car_manufacturer, car_model, car_year)"+
            " VALUES (?,?,?,?);";

        try {pStmnt = conn.prepareStatement(insertCarsTable);}
        catch (SQLException e) {
            System.out.println(e.getMessage());}

        String line = "";
        int i = 1;

        while ((line = br.readLine()) != null) {

            String[] cars = line.split(cvsSplitBy);

            for (int in=0 ; in < cars.length ; in++){ 
                cars[in] = cars[in].trim();}

            try{
                if(!cars[0].equals("?")){pStmnt.setInt(1,Integer.parseInt(cars[0]));}
                else                    {pStmnt.setNull(1,Types.INTEGER);}

                if(!cars[1].equals("?")){pStmnt.setString(2,(cars[1]));}
                else                    {pStmnt.setNull(2,Types.INTEGER);}

                if(!cars[2].equals("?")){pStmnt.setString(3,(cars[2]));}
                else                    {pStmnt.setNull(3,Types.INTEGER);}

                if(!cars[3].equals("?")){pStmnt.setInt(4,Integer.parseInt(cars[3]));}
                else                    {pStmnt.setNull(4,Types.INTEGER);}

            System.out.println(i++);
                // execute insert SQL stetement
                pStmnt.executeUpdate();}
            catch (SQLException e) {
                System.out.println("In exception need " + e.getMessage());
                continue;}
        }
    }
    public static void insertPersons( PreparedStatement pStmnt,
            BufferedReader br,
            Connection conn,
            String table,
            String cvsSplitBy) throws SQLException, IOException{
        String line = "";
        String insertPersonsTable = "INSERT INTO " + table +
            " (id,age,workclass,education,education_num,"+
            "marital_status,race,sex,capital_gain,"+
            "native_country)"+
            " VALUES (?,?,?,?,?,?,?,?,?,?);";

        try {pStmnt = conn.prepareStatement(insertPersonsTable);}
        catch (SQLException e) {
            System.out.println(e.getMessage());}

        int i = 1; 
        while ((line = br.readLine()) != null) {
            line.replaceAll("\\s+","");
            String[] persons = line.split(cvsSplitBy);

            System.out.println(i++);

            for (int in=0 ; in < persons.length ; in++){ 
                persons[in] = persons[in].trim();}

            try{

                if(!persons[0].equals("?")){pStmnt.setInt(1,Integer.parseInt(persons[0]));}
                else                    {pStmnt.setNull(1,Types.INTEGER);}
                if(!persons[1].equals("?")){pStmnt.setInt(2,Integer.parseInt(persons[1]));}
                else                    {pStmnt.setNull(2,Types.INTEGER);}
                if(!persons[2].equals("?")){pStmnt.setString(3,(persons[2]));}
                else                    {pStmnt.setNull(3,Types.VARCHAR);}
                if(!persons[3].equals("?")){pStmnt.setString(4,(persons[3]));}
                else                    {pStmnt.setNull(4,Types.VARCHAR);}
                if(!persons[4].equals("?")){pStmnt.setInt(5,Integer.parseInt(persons[4]));}
                else                    {pStmnt.setNull(5,Types.INTEGER);}
                if(!persons[5].equals("?")){pStmnt.setString(6,(persons[5]));}
                else                    {pStmnt.setNull(6,Types.VARCHAR);}
                if(!persons[6].equals("?")){pStmnt.setString(7,(persons[6]));}
                else                    {pStmnt.setNull(7,Types.VARCHAR);}
                if(!persons[7].equals("?")){pStmnt.setString(8,(persons[7]));}
                else                    {pStmnt.setNull(8,Types.VARCHAR);}
                if(!persons[8].equals("?")){pStmnt.setInt(9,Integer.parseInt(persons[8]));}
                else                    {pStmnt.setNull(9,Types.INTEGER);}
                if(!persons[9].equals("?")){pStmnt.setString(10,(persons[9]));}
                else                    {pStmnt.setNull(10,Types.VARCHAR);}


                // execute insert SQL stetement
                pStmnt.executeUpdate();}
            catch (SQLException e) {
                System.out.println("In persons exception need " + e.getMessage());}
        }
    }

    public static void insertRelations( PreparedStatement pStmnt,
            BufferedReader br,
            Connection conn,
            String table,
            String cvsSplitBy) throws SQLException, IOException{

        String line = "";
        String insertRelationsTable = "INSERT INTO " + table +
            " (id_person,id_relative, relationship)"+
            " VALUES (?,?,?);";

        try { pStmnt = conn.prepareStatement(insertRelationsTable);}
        catch (SQLException e) {
            System.out.println(e.getMessage());}

        int i = 1; 
        while ((line = br.readLine()) != null) {
            String[] relations = line.split(cvsSplitBy);
            System.out.println(i++);

            for (int in=0 ; in < relations.length ; in++){ 
                relations[in] = relations[in].trim();}

            try {

                if(!relations[0].equals("?")){pStmnt.setInt(1,Integer.parseInt(relations[0]));}
                else                    {pStmnt.setNull(1,Types.INTEGER);}
                if(!relations[1].equals("?")){pStmnt.setInt(2,Integer.parseInt(relations[1]));}
                else                    {pStmnt.setNull(2,Types.INTEGER);}
                if(!relations[2].equals("?")){pStmnt.setString(3,(relations[2]));}
                else                    {pStmnt.setNull(3,Types.VARCHAR);}

                // execute insert SQL stetement
                pStmnt.executeUpdate();}
            catch (SQLException e) {
                System.out.println("In Relations exception need " + e.getMessage());
                continue;}
        }
    }

    public static void insertCarsOwnedByPeople( PreparedStatement pStmnt,
            BufferedReader br,
            Connection conn,
            String table,
            String cvsSplitBy) throws SQLException, IOException{

        String line = "";
        String insertOwnedCarsTable = "INSERT INTO " + table +
            " (person_id, car_id, color, date_purchased)" +
            " VALUES (?,?,?,?);";


        try {pStmnt = conn.prepareStatement(insertOwnedCarsTable);}
        catch (SQLException e) {
            System.out.println(e.getMessage());}


        int i = 1; 
        while ((line = br.readLine()) != null) {
            /*line.trim();*/
            String[] ownedcars = line.split(cvsSplitBy);
            System.out.println(i++);

            for (int in=0 ; in < ownedcars.length ; in++){ 
                ownedcars[in] = ownedcars[in].trim();}

            try{
                if(!ownedcars[0].equals("?")){pStmnt.setInt(1,Integer.parseInt(ownedcars[0]));}
                else                    {pStmnt.setNull(1,Types.INTEGER);}
                if(!ownedcars[1].equals("?")){pStmnt.setInt(2,Integer.parseInt(ownedcars[1]));}
                else                    {pStmnt.setNull(2,Types.INTEGER);}
                if(!ownedcars[2].equals("?")){pStmnt.setString(3,(ownedcars[2]));}
                else                    {pStmnt.setNull(3,Types.VARCHAR);}
                if(!ownedcars[3].equals("?")){
                    String[] date = ownedcars[3].split("/");
                    pStmnt.setDate(4,Date.valueOf(
                                date[2]+"-"+date[1]+"-"+date[0]));}
                else{pStmnt.setNull(4,Types.DATE);}


                // execute insert SQL stetement
                pStmnt.executeUpdate();}
            catch (SQLException e) {
                System.out.println("In Owned Cars exception need " + e.getMessage());
                continue;}
        }
    }
}
