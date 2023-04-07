// References
//https://docs.oracle.com/javase/tutorial/jdbc/basics/index.html
// https://www.w3schools.com/xml/met_document_createelement.asp
// https://www.w3schools.com/xml/dom_document.asp
// https://docs.oracle.com/javase/7/docs/api/javax/xml/transform/TransformerFactory.html
//https://docs.oracle.com/javase/7/docs/api/javax/xml/transform/stream/StreamResult.html#:~:text=public%20class%20StreamResult%20extends%20Object,some%20other%20form%20of%20markup.

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;

public class summary {

    public static void main(String[] args) {

        // Get my identity information

        String username = "";
        String password = "";

        try {
            Scanner sc1 = new Scanner(System.in);
            username = sc1.next();
            password = sc1.next();

        } catch (Exception e) {
            System.out.println(e);
            return;
        }

        // Do the actual database work now

        Connection connect = null;
        Statement statement = null;
        ResultSet resultSet = null;
        ResultSet storeresultSet = null;
        ResultSet productresultSet = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Scanner sc = new Scanner(System.in);

            connect = DriverManager.getConnection("jdbc:mysql://db.cs.dal.ca:3306?serverTimezone=UTC&useSSL=false", username, password);
            statement = connect.createStatement();
            statement.execute("use athaker;");

            //Input the start date from the keyboard
            System.out.print("Enter date in the format (YYYY-MM-DD):");
            String start_date = sc.nextLine();

            // convert it into appropriate format
            DateTimeFormatter start_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDate starting_date = LocalDate.parse(start_date, start_formatter);
            System.out.println("Input date and time: " + starting_date);

            //Input the end date from the keyboard
            System.out.print("Enter date and time (YYYY-MM-DD):");
            String end_date = sc.nextLine();

            //convert it into appropriate format
            DateTimeFormatter end_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDate ending_date = LocalDate.parse(end_date, end_formatter);
            System.out.println("Input date and time: " + ending_date);

            //Input the name of the file from the keyboard
            String file_name = sc.nextLine();
            System.out.println(file_name);

            // sql query for getting CUSTOMER information
           String customer_info =
                    "SELECT CONCAT(customers.first_name, ' ', customers.last_name) AS full_name, " +
                    "customers.street, " +
                    "customers.city, " +
                    "customers.state, " +
                    "customers.zip_code, " +
                    "brand.brand_name, " +
                    "COUNT(orders.order_id) AS bicycles_purchased, " +
                    "SUM(order_items.list_price * order_items.quantity) AS order_value " +
                    "FROM customers customers " +
                    "JOIN orders orders ON customers.customer_id = orders.customer_id " +
                    "JOIN order_items order_items ON order_items.order_id = order_items.order_id " +
                    "JOIN products product ON order_items.product_id = product.product_id " +
                    "JOIN brands brand ON product.brand_id = brand.brand_id " +
                    "WHERE orders.order_date >= '2016-01-01' AND orders.order_date <= '2022-12-31' " +
                    "GROUP BY customers.first_name, customers.last_name, customers.street, customers.city, customers.state, customers.zip_code, brand.brand_name " +
                    "HAVING COUNT(orders.order_id) > 0";

           resultSet = statement.executeQuery(customer_info);

           // Building the xml file
           DocumentBuilderFactory document_factory = DocumentBuilderFactory.newInstance();
           DocumentBuilder doc_builder = document_factory.newDocumentBuilder();
           Document document = doc_builder.newDocument();

            // Create activity summary element - root element
            Element root = document.createElement("activity_summary");
            document.appendChild(root);

            // append timespan to activity summary element
            Element time_span = document.createElement("time_span");
            root.appendChild(time_span);

            // append start date to time span element
            Element start_date_xml = document.createElement("start_date");
            // include value in start date
            start_date_xml.appendChild(document.createTextNode(start_date));
            time_span.appendChild(start_date_xml);

            // append end date to time span element
            Element end_date_xml = document.createElement("end_date");
            // include value in end date
            end_date_xml.appendChild(document.createTextNode(end_date));
            time_span.appendChild(end_date_xml);

            // create element to hold the info of customers
            Element customer_list = document.createElement("customer_list");
            // append customer_list element to root
            root.appendChild(customer_list);

            // create element that holds the info of products
            Element product_list = document.createElement("product_list");
            // append product_list element to root node
            root.appendChild(product_list);

            // create element that holds the info of stores.
            Element store_list = document.createElement("store_list");
            // append store_list element to root node
            root.appendChild(store_list);


            while(resultSet.next()){
                Element customer = document.createElement("customer");
                customer_list.appendChild(customer);

                Element customer_name = document.createElement("customer_name");
                customer_name.appendChild(document.createTextNode(resultSet.getString("full_name")));
                customer.appendChild(customer_name);

                Element address = document.createElement("address");
                customer.appendChild(address);

                Element street_address = document.createElement("street_address");
                street_address.appendChild(document.createTextNode(resultSet.getString("street")));
                address.appendChild(street_address);

                Element city = document.createElement("city");
                city.appendChild(document.createTextNode(resultSet.getString("city")));
                address.appendChild(city);

                Element state = document.createElement("state");
                state.appendChild(document.createTextNode(resultSet.getString("state")));
                address.appendChild(state);

                Element zip_code = document.createElement("zip_code");
                zip_code.appendChild(document.createTextNode(resultSet.getString("zip_code")));
                address.appendChild(zip_code);

                Element order_value = document.createElement("order_value");
                order_value.appendChild(document.createTextNode(String.valueOf(resultSet.getDouble("order_value"))));
                customer.appendChild(order_value);

                Element bicycles_purchased = document.createElement("bicycles_purchased");
                bicycles_purchased.appendChild(document.createTextNode(String.valueOf(resultSet.getDouble("bicycles_purchased"))));
                customer.appendChild(bicycles_purchased);
            }

            // storing the product_information
            String product_info = "SELECT product.product_name AS product_name, " +
                    "brand.brand_name AS brand, " +
                    "category.category_name AS category, " +
                    "store.store_name AS store_name, " +
                    "SUM(order_items.quantity) AS units_sold " +
                    "FROM orders orders " +
                    "JOIN order_items order_items ON orders.order_id = order_items.order_id " +
                    "JOIN products product ON order_items.product_id = product.product_id " +
                    "JOIN brands brand ON product.brand_id = brand.brand_id " +
                    "JOIN categories category ON product.category_id = category.category_id " +
                    "JOIN stores store ON orders.store_id = store.store_id " +
                    "WHERE product.product_id NOT IN ( " +
                    "    SELECT DISTINCT order_items.product_id " +
                    "    FROM orders " +
                    "    JOIN order_items  ON orders.order_id = order_items.order_id " +
                    "    WHERE orders.order_date < '2016-01-01' " +
                    ") " +
                    "AND orders.order_date >= '2016-01-01' AND orders.order_date <= '2016-12-31' " +
                    "GROUP BY product.product_id, brand.brand_name, category.category_name, store.store_name " +
                    "HAVING MIN(orders.order_date) >= '2016-01-01' AND MIN(orders.order_date) <= '2016-12-31'";


            productresultSet = statement.executeQuery(product_info);

            // continue building xml for storing product information
            while(productresultSet.next()){
                Element new_product = document.createElement("new_product");
                root.appendChild(new_product);

                Element product_name = document.createElement("product_name");
                product_name.appendChild(document.createTextNode(productresultSet.getString("product_name")));
                new_product.appendChild(product_name);

                Element brand_name = document.createElement("brand");
                brand_name.appendChild(document.createTextNode(productresultSet.getString("brand")));
                new_product.appendChild(brand_name);

                Element category = document.createElement("category");
                category.appendChild(document.createTextNode(productresultSet.getString("category")));
                new_product.appendChild(category);

                Element store_sales = document.createElement("store_sales");
                new_product.appendChild(store_sales);

                Element store_name = document.createElement("store_name");
                store_name.appendChild(document.createTextNode(productresultSet.getString("store_name")));
                store_sales.appendChild(store_name);

                Element units_sold = document.createElement("units_sold");
                units_sold.appendChild(document.createTextNode(productresultSet.getString("units_sold")));
                store_sales.appendChild(units_sold);
            }

            // storing the information about the store

            String store_info = "SELECT " +
                    "store.store_name, " +
                    "store.city AS store_city, " +
                    "COUNT(staff.staff_id) AS employee_count, " +
                    "COUNT(orders.customer_id) AS customers_served, " +
                    "(SELECT CONCAT(customers.first_name, ' ', customers.last_name) AS customer_name FROM customers WHERE customers.customer_id = orders.customer_id) as customer_name, " +
                    "SUM(order_items.quantity * order_items.list_price * (1 - order_items.discount)) AS customer_sales_value " +
                    "FROM " +
                    "stores store " +
                    "LEFT JOIN staffs staff ON staff.store_id = store.store_id " +
                    "LEFT JOIN orders orders ON staff.store_id = orders.store_id " +
                    "LEFT JOIN order_items order_items ON orders.order_id = order_items.order_id " +
                    "WHERE " +
                    "orders.order_date >= '2016-01-01' AND orders.order_date <= '2022-12-31' " +
                    "GROUP BY " +
                    "store.store_id, orders.customer_id";

            storeresultSet = statement.executeQuery(store_info);

            while(storeresultSet.next()){
                // store_sales data
                Element store_name = document.createElement("store_name");
                store_name.appendChild(document.createTextNode(storeresultSet.getString("store_name")));
                store_list.appendChild(store_name);

                Element store_city = document.createElement("store_city");
                store_city.appendChild(document.createTextNode(storeresultSet.getString("store_city")));
                store_list.appendChild(store_city);

                Element employee_count = document.createElement("employee_count");
                employee_count.appendChild(document.createTextNode(storeresultSet.getString("employee_count")));
                store_list.appendChild(employee_count);

                Element customers_served = document.createElement("customers_served");
                customers_served.appendChild(document.createTextNode(storeresultSet.getString("customers_served")));
                store_list.appendChild(customers_served);

                Element customer_sales = document.createElement("customer_sales");
                store_list.appendChild(customer_sales);

                Element customer_name_store = document.createElement("customer_name");
                customer_name_store.appendChild(document.createTextNode(storeresultSet.getString("customer_name")));
                customer_sales.appendChild(customer_name_store);

                Element customer_sales_value = document.createElement("customer_sales_value");
                customer_sales_value.appendChild(document.createTextNode(storeresultSet.getString("customer_sales_value")));
                customer_sales.appendChild(customer_sales_value);

            }
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(document);
            File outputFile = new File("C:/Users/AVuser/IdeaProjects/5/athaker/src/"+file_name);
            FileWriter fileWriter = new FileWriter(outputFile);
            StreamResult result = new StreamResult(fileWriter);
            transformer.transform(source, result);


            statement.close();
            connect.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Connection failed");
            System.out.println(e.getMessage());
        }
    }
}
