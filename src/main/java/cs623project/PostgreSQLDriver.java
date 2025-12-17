package cs623project;

import java.io.IOException;
import java.sql.*;

public class PostgreSQLDriver {

    public static void main(String[] args) throws SQLException, IOException,
            ClassNotFoundException {
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres","postgres", "J4m1350nF4m1lyT1m3!" );
        conn.setAutoCommit(false);          //Atomicity
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);  //Isolation
        Statement stmt1=conn.createStatement();

        //Transaction #1 & 2: DELETE Queries

        //Validate Pre-transaction state
        ResultSet rs = stmt1.executeQuery("select * from product");
        System.out.println("-----Product PRE-----");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("prod_id")+" "+rs.getString("pname")+" "+rs.getDouble("price"));
        }
        System.out.println();
        rs = stmt1.executeQuery("select * from depot");
        System.out.println("-----Depot PRE-----");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("depid")+" Address: "+rs.getString("addr")+" volume "+rs.getDouble("volume"));
        }
        System.out.println();
        rs = stmt1.executeQuery("select * from stock");
        System.out.println("-----Stock PRE-----");
        while(rs.next()) {
            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depot ID: "+rs.getString("depid")+" Quantity "+rs.getDouble("quantity"));
        }

        //Execute transaction and confirm results
        String deleteProdString="DELETE FROM product WHERE prod_id=?";
        PreparedStatement deleteProd=conn.prepareStatement(deleteProdString);
        deleteProd.setString(1, "p1");
        deleteProd.executeUpdate(); //Transaction #1: The product p1 is deleted from Product and Stock.
        String deleteDepotString="DELETE FROM depot WHERE depid=?";
        PreparedStatement deleteDepot=conn.prepareStatement(deleteDepotString);
        deleteDepot.setString(1, "d1");
        deleteDepot.executeUpdate();
        stmt1.executeUpdate("DELETE FROM depot WHERE depid='d1'");              //Transaction #2: The depot d1 is deleted from Depot and Stock.

        String selectProduct="select * from product";
        PreparedStatement selectProd=conn.prepareStatement(selectProduct);
        rs=selectProd.executeQuery();
        System.out.println("-----Product POST #1-----");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("prod_id")+" "+rs.getString("pname")+" "+rs.getDouble("price"));
        }
        System.out.println();
        String selectDepotString="select * from depot";
        PreparedStatement selectDepot=conn.prepareStatement(selectDepotString);
        rs=selectDepot.executeQuery();
        System.out.println("-----Depot POST #1-----");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("depid")+" Address: "+rs.getString("addr")+" volume "+rs.getDouble("volume"));
        }
        System.out.println();
        String selectStockString="select * from stock";
        PreparedStatement selectStock=conn.prepareStatement(selectStockString);
        rs=selectStock.executeQuery();
        System.out.println("-----Stock POST #1-----");
        while(rs.next()) {
            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depot ID: "+rs.getString("depid")+" Quantity "+rs.getDouble("quantity"));
        }
        System.out.println();
        conn.rollback(); //Rollback changes

        // Transaction #3 & 4: UPDATE

        //Validate Pre-transaction state
        rs=selectProd.executeQuery();
        System.out.println("-----Product PRE-----");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("prod_id")+" "+rs.getString("pname")+" "+rs.getDouble("price"));
        }
        System.out.println();
        rs=selectDepot.executeQuery();
        System.out.println("-----Depot PRE-----");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("depid")+" Address: "+rs.getString("addr")+" volume "+rs.getDouble("volume"));
        }
        System.out.println();
        rs=selectStock.executeQuery();
        System.out.println("-----Stock PRE-----");
        while(rs.next()) {
            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depot ID: "+rs.getString("depid")+" Quantity "+rs.getDouble("quantity"));
        }
        System.out.println();

        //Execute transaction and confirm results
        String updateProductString="UPDATE product "+"SET prod_id = ? where prod_id = ?";
        PreparedStatement updateProd=conn.prepareStatement(updateProductString);
        updateProd.setString(1, "pp1");
        updateProd.setString(2, "p1");
        updateProd.executeUpdate();             //Transaction #3: The product p1 changes its name to pp1 in product and Stock.
        String updateDepotString="UPDATE depot "+"SET depid = ? where depid = ?";
        PreparedStatement updateDepot=conn.prepareStatement(updateDepotString);
        updateDepot.setString(1, "dd1");
        updateDepot.setString(2, "d1");
        updateDepot.executeUpdate();         //Transaction #4: The depot d1 changes its name to dd1 in Depot and Stock.

        rs=selectProd.executeQuery();
        System.out.println("-----Product POST #2-----");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("prod_id")+" "+rs.getString("pname")+" "+rs.getDouble("price"));
        }
        System.out.println();
        rs=selectDepot.executeQuery();
        System.out.println("-----Depot POST #2-----");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("depid")+" Address: "+rs.getString("addr")+" volume "+rs.getDouble("volume"));
        }
        System.out.println();
        rs = selectStock.executeQuery();
        System.out.println("-----Stock POST #2-----");
        while(rs.next()) {
            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depot ID: "+rs.getString("depid")+" Quantity "+rs.getDouble("quantity"));
        }
        conn.rollback(); //Rollback changes



        //Transaction #5: We add a product (p100, cd, 5) in Product and (p100, d2, 50) in Stock.
        //Transaction #6: We add a depot (d100, Chicago, 100) in Depot and (p1, d100, 100) in Stock.

        //Validate Pre-transaction state
        rs = selectProd.executeQuery();
        System.out.println("-----Product PRE-----");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("prod_id")+" "+rs.getString("pname")+" "+rs.getDouble("price"));
        }
        System.out.println();
        rs = selectDepot.executeQuery();
        System.out.println("-----Depot PRE-----");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("depid")+" Address: "+rs.getString("addr")+" volume "+rs.getDouble("volume"));
        }
        System.out.println();
        rs = selectStock.executeQuery();
        System.out.println("-----Stock PRE-----");
        while(rs.next()) {
            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depot ID: "+rs.getString("depid")+" Quantity "+rs.getDouble("quantity"));
        }
        System.out.println();

        //Execute transaction and confirm results
        String insertProductString="INSERT INTO product (prod_id, pname, price) VALUES (?,?,?)";
        PreparedStatement insertProd=conn.prepareStatement(insertProductString);
        insertProd.setString(1, "p100");
        insertProd.setString(2, "cd");
        insertProd.setDouble(3, 5);;
        insertProd.executeUpdate();
        String insertDepotString="INSERT INTO depot (depid, addr, volume) VALUES (?,?,?)";
        PreparedStatement insertDepot=conn.prepareStatement(insertDepotString);
        insertDepot.setString(1, "d100");
        insertDepot.setString(2, "Chicago");
        insertDepot.setDouble(3,100);
        insertDepot.executeUpdate();
        String insertProductStockString="INSERT INTO stock (prod_id, depid, quantity) VALUES (?,?,?)";
        PreparedStatement insertProdStock=conn.prepareStatement(insertProductStockString);
        insertProdStock.setString(1, "p100");
        insertProdStock.setString(2, "d2");
        insertProdStock.setDouble(3, 50);;
        insertProdStock.executeUpdate();
        String insertDepotStockString="INSERT INTO stock (prod_id, depid, quantity) VALUES (?,?,?)";
        PreparedStatement insertDepotStock=conn.prepareStatement(insertDepotStockString);
        insertDepotStock.setString(1, "p1");
        insertDepotStock.setString(2, "d100");
        insertDepotStock.setDouble(3, 100);;
        insertDepotStock.executeUpdate();

        rs = selectProd.executeQuery();
        System.out.println("-----Product POST #3-----");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("prod_id")+" "+rs.getString("pname")+" "+rs.getDouble("price"));
        }
        System.out.println();
        rs = selectDepot.executeQuery();
        System.out.println("-----Depot POST #3-----");
        while(rs.next()) {
            System.out.println("ID: " + rs.getString("depid")+" Address: "+rs.getString("addr")+" volume "+rs.getDouble("volume"));
        }
        System.out.println();
        rs = selectStock.executeQuery();
        System.out.println("-----Stock POST #3-----");
        while(rs.next()) {
            System.out.println("Prod ID: " + rs.getString("prod_id")+" Depot ID: "+rs.getString("depid")+" Quantity "+rs.getDouble("quantity"));
        }
        conn.rollback(); //Rollback changes

        stmt1.close();
        conn.close();

    }
}
