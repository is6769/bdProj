import java.sql.*;
public class TestConn{public static void main(String[]args)throws Exception{Class.forName("org.postgresql.Driver");try(Connection c=DriverManager.getConnection("jdbc:postgresql://localhost:5432/dbschemastudio","dbstudio","dbstudio")){System.out.println("ok");}}}
