
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.util.Date;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class JdbcExample 
{

    final static String driverClass = "oracle.jdbc.driver.OracleDriver";
    final static String connectionURL = "jdbc:oracle:thin:@vmlinux1:1521:TESTDB1";
    final static String userID = "scott";
    final static String userPassword = "tiger";
    Connection oraConnection  = null;
    SimpleDateFormat datePrintFormatPattern = null;
    SimpleDateFormat dateOracleFormatPattern = null;
    DecimalFormat decimalFormatPattern = null;

    class JdbcExampleException extends Exception 
    {
        
        private int intError;
        

         JdbcExampleException()
	 {
         }

        JdbcExampleException(String strMessage) 
	{
            super(strMessage);
        }

        JdbcExampleException(int intErrorNum) 
	{
            intError = intErrorNum;
        }


        JdbcExampleException(String strMessage, int intErrorNum) 
	{
            super(strMessage);
            intError = intErrorNum;
        }

        public String toString() 
	{
            return "JdbcExampleException ["+intError+"]";
        }

    }

    public JdbcExample() throws JdbcExampleException 
    {

        datePrintFormatPattern = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        dateOracleFormatPattern = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss a zZ");
        decimalFormatPattern = new DecimalFormat("#,###.##");
        
        try 
	{

            System.out.print("  Loading JDBC Driver    -> " + driverClass + "\n");
            Class.forName(driverClass).newInstance();

            System.out.print("  Connecting to          -> " + connectionURL + "\n");
            this.oraConnection = DriverManager.getConnection(connectionURL, userID, userPassword);
            System.out.print("  Connected as           -> " + userID + "\n");

        } 
	catch (ClassNotFoundException e) 
	{
            e.printStackTrace();
            throw new JdbcExampleException("ERROR: Class Not Found");
        } 
	catch (InstantiationException e) 
	{
            e.printStackTrace();
            throw new JdbcExampleException("ERROR: Instantiation Error");
        } 
	catch (IllegalAccessException e) 
	{
            e.printStackTrace();
            throw new JdbcExampleException("ERROR: Illegal Access");
        } 
	catch (SQLException sqlException) 
	{
            handleDatabaseException(sqlException, "Loading JDBC Driver", 1001);
        }

    }


    public void createTable() throws JdbcExampleException 
    {

        Statement stmt1 = null;
        ResultSet rset1 = null;

        String checkTableSQL = "SELECT 'EXISTS' " +
                               "FROM user_tables " +
                               "WHERE table_name = 'JDBC_EXAMPLE'";
        
        String createTableSQL =  "CREATE TABLE jdbc_example (" +
                                 "    object_id             NUMBER(15) " +
                                 "  , object_name           VARCHAR2(100) " +
                                 "  , real_number           NUMBER(15,2) " +
                                 "  , create_date           DATE " +
                                 "  , null_date             DATE " +
                                 "  , null_value            VARCHAR2(100))";
                                   
        try 
	{

            stmt1 = oraConnection.createStatement();
            rset1 = stmt1.executeQuery(checkTableSQL);

            if (rset1.next()) 
	    {
                if (rset1.getString(1).equals("EXISTS")) 
		{
                    System.out.print("  Found Existing Table   -> Calling dropTable()\n");
                    dropTable();
                }
            }

            rset1.close();
            stmt1.close();
            
            System.out.print("  Creating Table         -> ");
            stmt1 = oraConnection.createStatement();
            stmt1.executeUpdate(createTableSQL);
            System.out.print("Table created.\n");

        } 
	catch (SQLException sqlException) 4
	{
            if (sqlException.getErrorCode() == 955) 
	    {
                System.out.print("Table already exists!\n");
                throw new JdbcExampleException("ERROR: It appears the table \"jdbc_example\" still exists!");
            } 
	    else 
	    {
                handleDatabaseException(sqlException, "Creating Table \"jdbc_example\"", 1002);
            }
        } 
	finally 
	{
            try 
	    {
                stmt1.close();
            } 
	    catch (SQLException sqlException) 
	    {
                handleDatabaseException(sqlException, "ERROR: Closing statement.", 1003);
            }
        }

    }

    public void populateTable() throws JdbcExampleException 
    {

        Statement stmt = null;
        int insertResults = 0;
        int totalRecordsInserted = 0;
        Date startDate;
        long startTimeMillis = 0;
        long endTimeMillis = 0;

        String insertSQLStatement = "INSERT INTO jdbc_example ( " +
                                    "      object_id " +
                                    "    , object_name " +
                                    "    , real_number " +
                                    "    , create_date " +
                                    "    , null_date " +
                                    "    , null_value) " +
                                    "  SELECT " +
                                    "      object_id " +
                                    "    , object_name " +
                                    "    , 3.14 " +
                                    "    , created " +
                                    "    , null " +
                                    "    , null " +
                                    "  FROM all_objects";
        
        try 
	{

            startTimeMillis = System.currentTimeMillis();
            startDate = new Date(startTimeMillis);
            System.out.println();
            System.out.print("  Start Date / Time      -> " + datePrintFormatPattern.format(startDate) + "\n");

            System.out.print("  Populating Table       -> \n\n");

            stmt = oraConnection.createStatement();
            
            for (int i=0; i < 75; i++) 
	    {            
                insertResults = stmt.executeUpdate(insertSQLStatement);
                totalRecordsInserted += insertResults;
                System.out.print(".");
            }
    
            oraConnection.commit();
            System.out.println("\n");
            System.out.print("  # Rows Processed       -> " + decimalFormatPattern.format(totalRecordsInserted) + "\n");

            endTimeMillis = System.currentTimeMillis();
            printElapsedTime(startTimeMillis, endTimeMillis);

        } 
	catch (SQLException sqlException) 
	{
            handleDatabaseException(sqlException, "Populating Table \"jdbc_example\"", 1004);
        } 
	finally 
	{
            try 
	    {
                stmt.close();
            } 
	    catch (SQLException sqlException) 
	    {
                handleDatabaseException(sqlException, "ERROR: Closing statement.", 1005);
            }
        }

    }

    public void performQuery() throws JdbcExampleException 
    {

        Statement stmt = null;
        ResultSet rset = null;
        int rowCount = 0;
        Date startDate;
        long startTimeMillis = 0;
        long endTimeMillis = 0;
        int numTableRowsPerDot = 1000;
        int numDotsPerLine = 80;
        String queryString  = "SELECT " +
                              "   object_id " +
                              "  , object_name " +
                              "  , real_number " +
                              "  , create_date " +
                              "  , null_date " +
                              "  , null_value " +
                              "FROM jdbc_example " +
                              "ORDER BY object_id";

        try 
	{

            startTimeMillis = System.currentTimeMillis();
            startDate = new Date(startTimeMillis);
            System.out.println();
            System.out.print("  Start Date / Time      -> " + datePrintFormatPattern.format(startDate) + "\n");

            stmt = oraConnection.createStatement();
            rset = stmt.executeQuery(queryString);
            ResultSetMetaData rsMeta = rset.getMetaData();
            System.out.print("  # of Columns in Query  -> " + rsMeta.getColumnCount() + "\n");

            while (rset.next()) 
	    {

                int rowNumber = rset.getRow();
                rowCount++;

                if (rowCount == 1) 
		{
                    System.out.println();
                    System.out.println("  Printing First Row...");
                    System.out.println("  Row Number [" + rowNumber + "]");
                    System.out.println("  Row Count  [" + rowCount + "]");
                    System.out.println("  ---------------------");

                    int objectId = rset.getInt(1);
                    if (rset.wasNull()) 
		    {
			objectId = -1;
		    }

                    String objectName = rset.getString(2);
                    if (rset.wasNull()) 
		    {
			objectName = "<null>";
		    }
                    
                    float realNumber = rset.getFloat(3);
                    if (rset.wasNull()) 
		    {
			realNumber = 0.0f;
		    }
                    
                    Date createDate = rset.getTimestamp(4);
                    if (rset.wasNull()) 
		    {
			createDate = new Date(24L*60L*60L*1000L);
		    } // 1/1/1970 GMT

                    Date nullDateValue = rset.getTimestamp(5);
                    if (rset.wasNull()) 
		    {
			nullDateValue = new Date(24L*60L*60L*1000L);
		    } // 1/1/1970 GMT

                    String nullStringValue = rset.getString(6);
                    if (rset.wasNull()) 
		    {
			nullStringValue = "<null>";
		    }

                    System.out.println("      Object ID          -> " + objectId);
                    System.out.println("      Object Name        -> " + objectName);
                    System.out.println("      Real Number        -> " + realNumber);
                    System.out.println("      Create Date        -> " + dateOracleFormatPattern.format(createDate));
                    System.out.println("      Null Date          -> " + dateOracleFormatPattern.format(nullDateValue));
                    System.out.println("      Null Value         -> " + nullStringValue);
                    System.out.println();
                }

                if ((rowCount % numTableRowsPerDot) == 0 ) 
		{
                    System.out.print(".");
                }

                if ((rowCount % (numTableRowsPerDot*numDotsPerLine)) == 0) 
		{
                    System.out.println(" (" + decimalFormatPattern.format(rowCount) + " rows processed.)");
                }

            }

            rset.close();
            stmt.close();
                                                                                 
            System.out.println("\n");
            System.out.print("  # Rows Processed       -> " + decimalFormatPattern.format(rowCount) + "\n");
            
            endTimeMillis = System.currentTimeMillis();
            printElapsedTime(startTimeMillis, endTimeMillis);

        } 
	catch (SQLException sqlException) 
	{
            handleDatabaseException(sqlException, "Performing Query", 1006);
        }
    }

    public void dropTable() throws JdbcExampleException 
    {

        Statement stmt = null;

        String dropTableSQL = "DROP TABLE jdbc_example";

        try 
	{

            System.out.print("  Dropping Table         -> ");
            stmt = oraConnection.createStatement();
            stmt.executeUpdate(dropTableSQL);
            System.out.print("Table dropped.\n");

        } 
	catch (SQLException sqlException) 
	{
            handleDatabaseException(sqlException, "Dropping Table \"jdbc_example\"", 1007);
        } 
	finally 
	{
            try 
	    {
                stmt.close();
            } 
	    catch (SQLException sqlException) 
	    {
                handleDatabaseException(sqlException, "ERROR: Closing statement.", 1008);
            }
        }
    }

    public void closeConnection() throws JdbcExampleException 
    {

        try 
  	{
            System.out.print("  Closing Connection     -> ");
            if (this.oraConnection != null) 
	    {
                this.oraConnection.close();
            }
            System.out.print("Done.\n");
        } 
	catch (SQLException sqlException) 
	{
            handleDatabaseException(sqlException, "Loading JDBC Driver", 1009);
        }
    }


    public void handleDatabaseException(SQLException exception, String strMessage, int intErrorNum)
        throws JdbcExampleException 
    {
        
        System.err.println();
        System.err.println("+-------------------------------------------+");
        System.err.println("| Printing Exception Trace                  |");
        System.err.println("+-------------------------------------------+");
        exception.printStackTrace();
        
        System.err.println();
        System.err.println("+-------------------------------------------+");
        System.err.println("| Database Error Message                    |");
        System.err.println("+-------------------------------------------+");
        // System.err.println("Message    : " + exception.getMessage());
        System.err.println("Error Code : " + exception.getErrorCode());
        System.err.println("SQL State  : " + exception.getSQLState());

        System.err.println();
        System.err.println("+-------------------------------------------+");
        System.err.println("| Throwing Application Exception            |");
        System.err.println("+-------------------------------------------+");
        throw new JdbcExampleException("ERROR: " + strMessage, intErrorNum);

    }

    public void handleDatabaseException(SQLException exception, String strMessage)
        throws JdbcExampleException 
    {
        handleDatabaseException(exception, strMessage, -1);
    }

    public void printElapsedTime(long startTimeMillis, long endTimeMillis) 
    {

        long totalTimeMillis;

        totalTimeMillis = (endTimeMillis - startTimeMillis);
        long totalTimeSeconds = totalTimeMillis / 1000;
        String totalTimeSecondsStr = Integer.toString((int)(totalTimeSeconds % 60));  
        String totalTimeMinutesStr = Integer.toString((int)((totalTimeSeconds % 3600) / 60));  
        String totalTimeHoursStr = Integer.toString((int)(totalTimeSeconds / 3600));  
        for (int i = 0; i < 2; i++) 
	{  
            if (totalTimeSecondsStr.length() < 2) 
	    {  
                totalTimeSecondsStr = "0" + totalTimeSecondsStr;  
            }  
            if (totalTimeMinutesStr.length() < 2) 
	    {
                totalTimeMinutesStr = "0" + totalTimeMinutesStr;
            }
            if (totalTimeHoursStr.length() < 2) 
	    {
                totalTimeHoursStr = "0" + totalTimeHoursStr;
            }
        }

        System.out.print("  Processing Time        -> " +
                         totalTimeHoursStr + " hours " +
                         totalTimeMinutesStr  + " minutes " +
                         totalTimeSecondsStr + " seconds\n");
        System.out.print("                         -> " + totalTimeMillis + " milliseconds\n");

    }


    public static void main(String[] args) 
    {

        JdbcExample jdbcExample = null;
        
        try 
	{
            jdbcExample = new JdbcExample();
            jdbcExample.createTable();
            jdbcExample.populateTable();
            jdbcExample.performQuery();
            jdbcExample.dropTable();
        } 
	catch (JdbcExampleException e) 
	{
            System.err.println();
            System.err.println("+-------------------------------------------+");
            System.err.println("| Caught application exception in main().   |");
            System.err.println("| Printing trace.                           |");
            System.err.println("+-------------------------------------------+");
            e.printStackTrace();
        } 
	finally 
	{
            if (jdbcExample.oraConnection != null) 
	    {
                try 
		{
                    jdbcExample.closeConnection();            
                } 
		catch (JdbcExampleException e) 
		{
                    e.printStackTrace();
                }
            }
        } 
    } 
}
