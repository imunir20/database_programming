import java.io.*;
import java.sql.*;
import java.util.*;
import oracle.jdbc.driver.*;
import org.apache.ibatis.jdbc.ScriptRunner;

public class Student {
    static Connection con;
    static Statement stmt;

    static ArrayList <Integer> inputs = new ArrayList<>();
    static ArrayList <Double> fInputs = new ArrayList<>();


    public static void main(String argv[])
    {
        boolean success = false;
        int choice = -1;
        ArrayList <PreparedStatement> statements = new ArrayList<>();

        while (!success) {
            String u = promptLogin(0);
            String p = promptLogin(1);
            success = connectToDatabase(u, p);
        }



        success = false;
        while (!success) {
            success = runScript();
        }

        while (true) {
            choice = printMenu();
            printSpecificMenu(choice);
            System.out.println();
        }


    }

    private static int checkValid(String input) {

        input = input.toUpperCase();
        if (input.equals("YES")) {
            return 1;
        }
        else if (input.equals("NO")) {
            return 0;
        }
        else {
            System.err.println("Invalid option entered. Please enter \"Yes\" or \"No\"");
            return -1;
        }
    }


    public static void clearStatements(ArrayList <PreparedStatement> state) {

        for (int k = 0; k < state.size(); k++) {
            try {
                state.get(k).close();
            }
            catch (Exception e) {
                System.err.println("Error occurred when closing statements.");
            }
        }
        state.clear();

    }


    public static void runQueries(ArrayList <PreparedStatement> queries, ArrayList <Integer> params) throws SQLException {

        int currInput = 1;
        for (int k = 0; k < queries.size(); k++) {       // Go through all queries
            for (int w = 0; w < params.size(); w++) {        // Set all appropriate parameters of query
                if (params.get(w) != -1) {
                    queries.get(k).setInt(currInput, params.get(w));
                    currInput++;
                } else if(w == 1 && fInputs.size() > 0) {
                    queries.get(k).setDouble(currInput, fInputs.get(0));
                    currInput++;
                }
            }
            currInput = 0;
            ResultSet rs = queries.get(k).executeQuery();

            System.out.print(rs.getMetaData().getColumnName(1));
            for (int z = 2; z <= rs.getMetaData().getColumnCount(); z++) {
                System.out.print(", " + rs.getMetaData().getColumnName(z));
            }
            System.out.println("");
            while (rs.next()) {
                System.out.print(rs.getString(1));
                for (int g = 2; g <= rs.getMetaData().getColumnCount(); g++) {
                    System.out.print(", " + rs.getString(g));
                }
                System.out.println();
            }
            System.out.println("\n");
            rs.close();
        }
        clearStatements(queries);
        params.clear();
        fInputs.clear();
    }

    public static int promptField(String field) {

        Scanner in = new Scanner(System.in);
        int valid = -1;
        String choice = "";

        while (valid == -1) {
            System.out.println(field + " (Yes/No): ");
            choice = in.nextLine();
            valid = checkValid(choice);
            if (valid == 1) {
                return 1;
            } else if (valid == 0) {
                return 0;
            }
        }
        return 0;
    }



    public static double promptNumField(String field) {

        Scanner in = new Scanner(System.in);
        int valid = -1;
        int choiceNum = -1;
        double choice2 = -1.0;
        String choice = "";

        while (choiceNum == -1) {
            System.out.println(field + " (-1 to skip): ");
            choice = in.nextLine();
            try {
                if (!field.equals("total")) {
                    choiceNum = Integer.parseInt(choice);
                    if (choiceNum == -1) {
                        return -1;
                    }
                }
                else {
                    choice2 = Double.parseDouble(choice);
                    if (choice2 < 0) {
                        return -1;
                    }
                    return choice2;
                }
            }
            catch (Exception e) {
                choiceNum = -1;
                System.err.println("Invalid number entered. Please try again.");
            }
        }
        return choiceNum;
    }


    public static String buildQuery(ArrayList <Integer> outs, int dist) {

        int total = 0;
        String cQuery = "SELECT ";
        String fClause = " FROM TRANSACTIONS T, TRANSACTION_CONTAINS TC ";
        String wClause = "WHERE T.transaction_ID = TC.transaction_id";
        String [] oFields = {"T.transaction_ID", "T.customer_ID", "T.transaction_date",
                    "T.payment_method", "T.total", "TC.UPC", "TC.quantity"};
        String [] iFields = {"T.customer_ID", "T.total", "TC.UPC", "TC.quantity"};

        if (dist == 1) {
            cQuery = cQuery + "DISTINCT ";
        }

        for (int x = 0; x < outs.size(); x++) {
            if (outs.get(x) == 1) {
                if (total > 0) {
                    cQuery = cQuery + ", " + oFields[x];
                }
                else {
                    cQuery = cQuery + oFields[x];
                    total = total + 1;
                }
            }
        }
        for (int y = 0; y < inputs.size(); y++) {
            if (inputs.get(y) != -1) {
                wClause = wClause + " AND " + iFields[y] + " = ?";
            }
            else if (y == 1 && fInputs.size() > 0) {
                wClause = wClause + " AND " + iFields[y] + " = ?";
            }
        }

        cQuery = cQuery + fClause + wClause;
        return cQuery;
    }




    public static String promptDetailedFields() {

        int numIn = 0;
        int numOut = 0;
        int distinct = 0;
        String fQuery = "";
        ArrayList <Integer> outputs = new ArrayList<>();
        inputs = new ArrayList<>();

        System.out.println("Input fields: ");
        inputs.add((int)promptNumField("customer_ID"));
        fInputs.add(promptNumField("total"));
        inputs.add(-1);
        inputs.add((int)promptNumField("UPC"));
        inputs.add((int)promptNumField("quantity"));

        if(fInputs.get(0) < 0) {
            fInputs.clear();
        }

        System.out.println("\nOutput fields: ");

        outputs.add(promptField("transaction_ID"));
        outputs.add(promptField("customer_ID"));
        outputs.add(promptField("transaction_date"));
        outputs.add(promptField("payment_method"));
        outputs.add(promptField("total"));
        outputs.add(promptField("UPC"));
        outputs.add(promptField("quantity"));

        distinct = promptField("Distinct");

        fQuery = buildQuery(outputs, distinct);
        return fQuery;

    }


    public static void printSpecificMenu(int opt) {

        Scanner in = new Scanner(System.in);
        ArrayList <PreparedStatement> execute= new ArrayList<>();
        //ArrayList <Integer> params = new ArrayList<>();
        int valid = -1;
        int index = 0;
        int choiceNum = -1;
        String choice = "";
        String query = "";

        if (opt == 1) {
            try {
                while (valid == -1) {
                    System.out.println("Product (Yes/No): ");
                    choice = in.nextLine();
                    valid = checkValid(choice);
                    if (valid == 1) {
                        execute.add(con.prepareStatement("SELECT * FROM PRODUCT"));
                    }
                }

                valid = -1;
                while (valid == -1) {
                    System.out.println("Customer (Yes/No): ");
                    choice = in.nextLine();
                    valid = checkValid(choice);
                    if (valid == 1) {
                        execute.add(con.prepareStatement("SELECT * FROM CUSTOMER"));
                    }
                }

                valid = -1;
                while (valid == -1) {
                    System.out.println("Transactions (Yes/No): ");
                    choice = in.nextLine();
                    valid = checkValid(choice);
                    if (valid == 1) {
                        execute.add(con.prepareStatement("SELECT * FROM TRANSACTIONS"));
                    }
                }

                valid = -1;
                while (valid == -1) {
                    System.out.println("Transaction_Contains (Yes/No): ");
                    choice = in.nextLine();
                    valid = checkValid(choice);
                    if (valid == 1) {
                        execute.add(con.prepareStatement("SELECT * FROM TRANSACTION_CONTAINS"));
                    }
                }
            }
            catch (Exception e) {
                System.err.println("Error in option 1.");
            }
            try {
                runQueries(execute, inputs);
                //clearStatements(execute);
            }
            catch (Exception e) {
                System.err.println("Error when running queries for option 1.");
            }
        }


        else if (opt == 2) {

            while (choiceNum == -1) {
                System.out.println("Please enter a transaction_id: ");
                choice = in.nextLine();

                try {
                    choiceNum = Integer.parseInt(choice);
                    inputs.add(choiceNum);
                    execute.add(con.prepareStatement("SELECT T.*, AVG(TC.QUANTITY) FROM TRANSACTIONS T, TRANSACTION_CONTAINS TC " +
                            "WHERE T.TRANSACTION_ID = ? AND " +
                            "T.TRANSACTION_ID = TC.TRANSACTION_ID GROUP BY T.TRANSACTION_ID, T.CUSTOMER_ID, T.TRANSACTION_DATE, T.PAYMENT_METHOD, T.TOTAL"));

                    runQueries(execute, inputs);
                }
                catch (Exception e) {
                    choiceNum = -1;
                    System.err.println("Invalid transaction_id entered. Please try again.");
                }
            }


        }
        else if (opt == 3) {
            try {
                execute.add(con.prepareStatement(promptDetailedFields()));
                //System.out.println(execute.get(0));
                //System.out.println(params.get(0));
                runQueries(execute, inputs);
            }
            catch (Exception e) {
                System.err.println("Error occurred when attempting to build option 4 query.");
            }
        }

        else {
            try {
                con.close();
            }
            catch (Exception e) {
                System.err.println("Error made when attempting to close the connection");
            }
            System.exit(0);
        }

    }


    public static int printMenu() {

        Scanner in = new Scanner(System.in);
        int choiceNum = -1;
        String choice = "";

        while (!(choiceNum >= 1 && choiceNum <= 4)) {
            System.out.println("1. View table contents");
            System.out.println("2. Search by Transaction_ID");
            System.out.println("3. Search by one or more attributes");
            System.out.println("4. Exit");
            choice = in.nextLine();
            try {
                choiceNum = Integer.parseInt(choice);
            }
            catch (Exception e) {
                choiceNum = -1;
            }

            if(!(choiceNum >= 1 && choiceNum <= 4)) {
                System.err.println("Invalid option entered. Please enter a number from 1-4 to continue.");
            }
        }
        System.out.println("\n");
        return choiceNum;
    }



    public static String promptLogin(int toggle) {

        Scanner in = new Scanner(System.in);
        String cred = "";
        switch(toggle) {
            case 0:
                System.out.println("Please enter your username: ");
                cred = in.nextLine();
                return cred;
            case 1:
                System.out.println("Please enter your password: ");
                cred = in.nextLine();
                return cred;
        }
        return "";
    }




    public static boolean runScript() {

        Scanner in = new Scanner(System.in);
        String fPath = "";
        System.out.println("Please enter the path to the oracle.sql file.");
        fPath = in.nextLine();

        try {
            ScriptRunner sr = new ScriptRunner(con);
            //File f = new File("oracle.sql");
            //String p = f.getAbsolutePath();
            //System.out.println(p);

            Reader reader = new BufferedReader(new FileReader(fPath));

            sr.runScript(reader);
        }
        catch(Exception e) {
            //e.printStackTrace();
            System.err.println("Invalid file path given. Please try again.");
            return false;
        }
        return true;

    }


    public static boolean connectToDatabase(String username, String password)
    {
	String driverPrefixURL="jdbc:oracle:thin:@";
	String jdbc_url="artemis.vsnet.gmu.edu:1521/vse18c.vsnet.gmu.edu";

        // IMPORTANT: DO NOT PUT YOUR LOGIN INFORMATION HERE. INSTEAD, PROMPT USER FOR HIS/HER LOGIN/PASSWD


        try{
	    //Register Oracle driver
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        } catch (Exception e) {
            System.out.println("Failed to load JDBC/ODBC driver.");
            return false;
        }

       try{
            System.out.println(driverPrefixURL+jdbc_url);
            con = DriverManager.getConnection(driverPrefixURL+jdbc_url, username, password);
            DatabaseMetaData dbmd=con.getMetaData();
            stmt=con.createStatement();

            System.out.println("Connected.");

            if(dbmd==null){
                System.out.println("No database meta data");
            }
            else {
                System.out.println("Database Product Name: "+dbmd.getDatabaseProductName());
                System.out.println("Database Product Version: "+dbmd.getDatabaseProductVersion());
                System.out.println("Database Driver Name: "+dbmd.getDriverName());
                System.out.println("Database Driver Version: "+dbmd.getDriverVersion());
            }
        }
       catch(Exception e) {
           //e.printStackTrace();
           System.err.println("Invalid username or password. Please try again.");
           return false;
       }
       return true;

    }// End of connectToDatabase()
}// End of class

