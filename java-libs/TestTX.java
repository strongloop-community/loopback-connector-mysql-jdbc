//STEP 1. Import required packages
import java.sql.*;

public class TestTX {
   // JDBC driver name and database URL
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
   static final String DB_URL = "CHANGEME";

   //  Database credentials
   static final String USER = "test";
   static final String PASS = "CHANGEME";

 public static void main(String[] args) {
   Connection conn = null;
   Statement stmt = null;
   try{
      //STEP 2: Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      //STEP 3: Open a connection
      System.out.println("Connecting to database...");
      conn = DriverManager.getConnection(DB_URL,USER,PASS);

      //STEP 4: Set auto commit as false.
      conn.setAutoCommit(false);

      //STEP 5: Execute a query to create statment with
      // required arguments for RS example.
      System.out.println("Creating statement...");
      stmt = conn.createStatement(
                           ResultSet.TYPE_SCROLL_INSENSITIVE,
                           ResultSet.CONCUR_UPDATABLE);

      //STEP 6: INSERT a row into Employees table
      System.out.println("Inserting one row....");
      String SQL = "INSERT INTO PostTX(title,content) VALUES('t1', 'c1')";
      stmt.executeUpdate(SQL);

      //STEP 8: Commit data here.
      System.out.println("Rollbacking data here....");
      conn.rollback();

	  //STEP 9: Now list all the available records.
      String sql = "SELECT * FROM PostTX";
      ResultSet rs = stmt.executeQuery(sql);
      System.out.println("List result set for reference....");
      printRs(rs);

      //STEP 10: Clean-up environment
      rs.close();
      stmt.close();
      conn.close();
   }catch(SQLException se){
      //Handle errors for JDBC
      se.printStackTrace();
      // If there is an error then rollback the changes.
      System.out.println("Rolling back data here....");
	  try{
		 if(conn!=null)
            conn.rollback();
      }catch(SQLException se2){
         se2.printStackTrace();
      }//end try

   }catch(Exception e){
      //Handle errors for Class.forName
      e.printStackTrace();
   }finally{
      //finally block used to close resources
      try{
         if(stmt!=null)
            stmt.close();
      }catch(SQLException se2){
      }// nothing we can do
      try{
         if(conn!=null)
            conn.close();
      }catch(SQLException se){
         se.printStackTrace();
      }//end finally try
   }//end try
   System.out.println("Goodbye!");
}//end main

   public static void printRs(ResultSet rs) throws SQLException{
      //Ensure we start with first row
      rs.beforeFirst();
      while(rs.next()){
         //Retrieve by column name
         int id  = rs.getInt("id");
         String first = rs.getString("title");
         String last = rs.getString("content");

         //Display values
         System.out.print("ID: " + id);
         System.out.print(", First: " + first);
         System.out.println(", Last: " + last);
     }
     System.out.println();
   }//end printRs()
}//end JDBCExample
