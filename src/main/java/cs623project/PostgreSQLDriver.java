package cs623project;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreSQLDriver {

    public static void main(String[] args) throws SQLException, IOException,
            ClassNotFoundException {
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres","postgres", "J4m1350nF4m1lyT1m3!" );
        conn.setAutoCommit(false);
        Statement stmt1=conn.createStatement();

//        //Transaction #1: The product p1 is deleted from Product and Stock.
//
//        //Validate Pre-transaction state
//        ResultSet rs = stmt1.executeQuery("select * from product");
//        while(rs.next()) {
//            System.out.println("ID: " + rs.getString("prod_id")+" "+rs.getString("pname")+" "+rs.getDouble("price"));
//        }
//        rs = stmt1.executeQuery("select * from stock");
//        while(rs.next()) {
//            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depeot ID: "+rs.getString("depid")+" Quantity"+rs.getDouble("quantity"));
//        }
//
//        //Execute transaction and confirm results
//        stmt1.executeUpdate("DELETE FROM product WHERE prod_id='p1'");
//        rs = stmt1.executeQuery("select * from product");
//        while(rs.next()) {
//            System.out.println("ID: " + rs.getString("prod_id")+" "+rs.getString("pname")+" "+rs.getDouble("price"));
//        }
//        rs = stmt1.executeQuery("select * from stock");
//        while(rs.next()) {
//            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depeot ID: "+rs.getString("depid")+" Quantity"+rs.getDouble("quantity"));
//        }
//        conn.rollback(); //Rollback changes
//        System.out.println("ROLLBACK");
//        rs = stmt1.executeQuery("select * from product");
//        while(rs.next()) {
//            System.out.println("ID: " + rs.getString("prod_id")+" "+rs.getString("pname")+" "+rs.getDouble("price"));
//        }
//        rs = stmt1.executeQuery("select * from stock");
//        while(rs.next()) {
//            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depeot ID: "+rs.getString("depid")+" Quantity"+rs.getDouble("quantity"));
//        }
//
//        //Transaction #2: The depot d1 is deleted from Depot and Stock.
//
//        //Validate Pre-transaction state
//        ResultSet rs = stmt1.executeQuery("select * from depot");
//        while(rs.next()) {
//            System.out.println("ID: " + rs.getString("depid")+" Address: "+rs.getString("addr")+" volume"+rs.getDouble("volume"));
//        }
//        rs = stmt1.executeQuery("select * from stock");
//        while(rs.next()) {
//            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depeot ID: "+rs.getString("depid")+" Quantity"+rs.getDouble("quantity"));
//        }
//
//        //Execute transaction and confirm results
//        stmt1.executeUpdate("DELETE FROM depot WHERE depid='d1'");
//        rs = stmt1.executeQuery("select * from depot");
//        while(rs.next()) {
//            System.out.println("ID: " + rs.getString("depid")+" Address: "+rs.getString("addr")+" volume"+rs.getDouble("volume"));
//        }
//        rs = stmt1.executeQuery("select * from stock");
//        while(rs.next()) {
//            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depeot ID: "+rs.getString("depid")+" Quantity"+rs.getDouble("quantity"));
//        }
//        conn.rollback(); //Rollback changes
//        System.out.println("ROLLBACK");
//        rs = stmt1.executeQuery("select * from depot");
//        while(rs.next()) {
//            System.out.println("ID: " + rs.getString("depid")+" Address: "+rs.getString("addr")+" volume"+rs.getDouble("volume"));
//        }
//        rs = stmt1.executeQuery("select * from stock");
//        while(rs.next()) {
//            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depeot ID: "+rs.getString("depid")+" Quantity"+rs.getDouble("quantity"));
//        }

        // Transaction #3The product p1 changes its name to pp1 in Product and Stock.

        //Validate Pre-transaction state
        ResultSet rs = stmt1.executeQuery("select * from product");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("prod_id")+" "+rs.getString("pname")+" "+rs.getDouble("price"));
        }
        rs = stmt1.executeQuery("select * from stock");
        while(rs.next()) {
            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depot ID: "+rs.getString("depid")+" Quantity"+rs.getDouble("quantity"));
        }

        //Execute transaction and confirm results
        stmt1.executeUpdate("UPDATE product SET prod_id='pp1' where prod_id='p1'");
        rs = stmt1.executeQuery("select * from product");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("prod_id")+" "+rs.getString("pname")+" "+rs.getDouble("price"));
        }
        rs = stmt1.executeQuery("select * from stock");
        while(rs.next()) {
            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depot ID: "+rs.getString("depid")+" Quantity"+rs.getDouble("quantity"));
        }
        conn.rollback(); //Rollback changes
        System.out.println("ROLLBACK");
        rs = stmt1.executeQuery("select * from product");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("prod_id")+" "+rs.getString("pname")+" "+rs.getDouble("price"));
        }
        rs = stmt1.executeQuery("select * from stock");
        while(rs.next()) {
            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depot ID: "+rs.getString("depid")+" Quantity"+rs.getDouble("quantity"));
        }
        stmt1.close();
        conn.close();

    }
}
