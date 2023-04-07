import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
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

            DateTimeFormatter start_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDate starting_date = LocalDate.parse(start_date, start_formatter);
            System.out.println("Input date and time: " + starting_date);

            //Input the end date from the keyboard
            System.out.print("Enter date and time (YYYY-MM-DD):");
            String end_date = sc.nextLine();

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

            // create element to hold all list of customers
            Element customer_list = document.createElement("customer_list");
            // append customer_list element to root
            root.appendChild(customer_list);

            Element product_list = document.createElement("product_list");
            root.appendChild(product_list);

            Element store_list = document.createElement("store_list");
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
                System.out.println(resultSet.getString("state"));
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
         /*
            String product_info = "SELECT p.product_name AS ProductName, b.brand_name AS BrandName, "
                    + "c.category_name AS CategoryName, s.store_name AS StoreName, "
                    + "SUM(oi.quantity) AS UnitsSold "
                    + "FROM orders o "
                    + "JOIN order_items oi ON o.order_id = oi.order_id "
                    + "JOIN products p ON oi.product_id = p.product_id "
                    + "JOIN brands b ON p.brand_id = b.brand_id "
                    + "JOIN categories c ON p.category_id = c.category_id "
                    + "JOIN stores s ON o.store_id = s.store_id "
                    + "WHERE p.product_id NOT IN ("
                    + "SELECT DISTINCT oi2.product_id "
                    + "FROM orders o2 "
                    + "JOIN order_items oi2 ON o2.order_id = oi2.order_id "
                    + "WHERE o2.order_date < '2023-01-01') "
                    + "AND o.order_date BETWEEN '2023-01-01' AND '2023-03-31' "
                    + "GROUP BY p.product_id, b.brand_name, c.category_name, s.store_name "
                    + "HAVING MIN(o.order_date) BETWEEN '2023-01-01' AND '2023-03-31'";

            resultSet = statement.executeQuery(product_info);
            */
            // Process the ResultSet object and print out the results
           /* while (resultSet.next()) {
                String productName = resultSet.getString("ProductName");
                String brandName = resultSet.getString("BrandName");
                String categoryName = resultSet.getString("CategoryName");
                String storeName = resultSet.getString("StoreName");
                int unitsSold = resultSet.getInt("UnitsSold");

                System.out.println(productName + ", " + brandName + ", " + categoryName + ", " + storeName + ", " + unitsSold);
            }
            */
            // sql query for getting- Store Information

               /*
                // product_information
                Element new_product = document.createElement("new_product");
                root.appendChild(new_product);

                Element product_name = document.createElement("product_name");
              //  product_name.appendChild(document.createTextNode(resultSet.getString("")));
                new_product.appendChild(product_name);

                Element brand_name = document.createElement("brand");
                //brand_name.appendChild(document.createTextNode(resultSet.getString("")));
                new_product.appendChild(brand_name);

               // String[] categories = resultSet.getString("category_names").split(", ");
               // for (String i : categories) {
                    Element category_final = document.createElement("category");
                 //   category_final.appendChild(document.createTextNode(""));
                    new_product.appendChild(category_final);


                Element store_sales = document.createElement("store_sales");
                new_product.appendChild(store_sales);

                Element units_sold = document.createElement("units_sold");
                store_sales.appendChild(units_sold);
                */
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
                //store_city.appendChild(document.createTextNode(storeresultSet.getString("store_city")));
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
                customer_sales.appendChild(customer_name_store);

                Element customer_sales_value = document.createElement("customer_sales_value");
                customer_sales.appendChild(customer_sales_value);

                /*   String store_name = resultSet.getString("store_name");
                String city = resultSet.getString("city");
                int employee_count = resultSet.getInt("employee_count");
                int customers_served = resultSet.getInt("customers_served");
                String customer_name = resultSet.getString("customer_name");
                double customer_sales_value = resultSet.getDouble("customer_sales_value");

                // Display the store information
                System.out.println("Store Name: " + store_name);
                System.out.println("City: " + city);
                System.out.println("Number of Employees: " + employee_count);
                System.out.println("Number of Customers served by the employee: " + customers_served);
                System.out.println("Customer name"+customer_name);
                System.out.println("Total Sales: " + customer_sales_value);
                System.out.println();

              */
            }
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(document);
            File outputFile = new File("columns.xml");
            FileWriter fileWriter = new FileWriter(outputFile);
            StreamResult result = new StreamResult(fileWriter);
            transformer.transform(source, result);

/*
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnCount = rsmd.getColumnCount();

            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();

            Element rootElement = doc.createElement("activity_summary");
            doc.appendChild(rootElement);

            Element columnElement = doc.createElement("time_span");
            columnElement.setAttribute("name", rsmd.getColumnName(i));
            columnElement.setAttribute("type", rsmd.getColumnTypeName(i));
            rootElement.appendChild(columnElement);

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File("columns.xml"));
            transformer.transform(source, result);
*/
            statement.close();
            connect.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Connection failed");
            System.out.println(e.getMessage());
        }
    }
}
