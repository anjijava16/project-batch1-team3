package com.orienit.kalyan.hadoop.training.mapreduce.assignment3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
//import mrdp.logging.LogWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
public class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
    // Fprivate Text videoName = new Text();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            InputStream is = new ByteArrayInputStream(value.toString().getBytes());
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder;
			try {
				   Connection conn = null;
				   Statement stmt = null;
					Class.forName("com.mysql.jdbc.Driver"); 
					conn = DriverManager.getConnection("jdbc:mysql://localhost/kalyan", "root", "root");
					stmt = conn.createStatement();
				dBuilder = dbFactory.newDocumentBuilder();
				Document doc;
				doc = dBuilder.parse(is);
				doc.getDocumentElement().normalize();
	            NodeList nList = doc.getElementsByTagName("record");
	            for (int temp = 0; temp < nList.getLength(); temp++) {
	                Node nNode = nList.item(temp);
	                Element eElement = (Element) nNode;
	                String dt = eElement.getElementsByTagName("dt").item(0).getTextContent();
                    String status = eElement.getElementsByTagName("status").item(0).getTextContent();
                    String ip = eElement.getElementsByTagName("ip").item(0).getTextContent();
                    String country = eElement.getElementsByTagName("country").item(0).getTextContent();
                    
	                if (status.equals("SUCCESS")) {
	                   
	                	  String sql = "insert into eventlog (dt,status,ip,country) values ('" + dt +"','" + status + "','" + ip +"','" + country + "')";
    	                  stmt.executeUpdate(sql);	
	                      System.out.println(dt + "," + status + "," + ip +","+country);
	                      context.write(new Text(dt + "," + status + "," + ip +","+country), NullWritable.get());
	                }
	            }
			}  
             catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
        }
    }

