package azkaban.viewer.hive;

import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.utils.Props;
import azkaban.webapp.servlet.Page;
import azkaban.server.session.Session;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;

import org.apache.hive.jdbc.HiveDriver;

public class HiveViewerServlet extends LoginAbstractAzkabanServlet {

  private final Props props;
  private final String viewerName;
  private final String viewerPath;
  private final String viewerDbUrl;
  private final String viewerDbUser;
  private final String viewerDbPass;

  public HiveViewerServlet(final Props props) {
    super(new ArrayList<>());
    this.props = props;
    this.viewerName = props.getString("viewer.name");
    this.viewerPath = props.getString("viewer.path");
    this.viewerDbUrl = props.getString("viewer.dburl");
    this.viewerDbUser = props.getString("viewer.dbuser");
    this.viewerDbPass = props.getString("viewer.dbpass");
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {

    super.init(config);
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    
    final boolean ajax = hasParam(req, "ajax");
    try {
      if (ajax) {
        String ajaxParam = getParam(req, "ajax");
        if (ajaxParam.equals("sqlStatement")) {
          final String sqlStatement = getParam(req, "sqlexec");
          Map<String, Object> ret = new HashMap<>();
          List<String> headerEles = new ArrayList<>();
          List<List<String>> rowEles = new ArrayList<>();
          List<List<String>> dataEles = new ArrayList<>();
  
          ResultSet rs = getResultSet(sqlStatement);
          ResultSetMetaData rsmd = getResultSetMetaData(rs);
          int count = rsmd.getColumnCount();
  
          for(int i = 1; i<=count; i++) {
            headerEles.add(rsmd.getColumnName(i));
          } 
  
          while (rs.next()) {
            List<String> row = new ArrayList<>();
            for (int i = 1; i <= count; i ++) {
              row.add(rs.getObject(i).toString());
            } 
            dataEles.add(row);
          }
          ret.put("header", headerEles);
          ret.put("data", dataEles);
          
          this.writeJSON(resp, ret);
        } else if (ajaxParam.equals("getDbNames")) {
          ResultSet rs = getResultSet("show databases");
          List<String> dbNames = new ArrayList<>();
          Map<String, Object> ret = new HashMap<>();
          while (rs.next()) {
            dbNames.add(rs.getString(1));
          }
          ret.put("dbnames", dbNames);
          this.writeJSON(resp, ret);
        } else if (ajaxParam.equals("getTableNames")) {
          final String dbname = getParam(req, "db");
          List<String> tbNames = new ArrayList<>();
          Map<String, Object> ret = new HashMap<>();
          ResultSet rs = getResultSet("use " + dbname, "show tables");
          while (rs.next()) {
            tbNames.add(rs.getString(2));
          }
          ret.put("tables", tbNames);
          this.writeJSON(resp, ret);
        }
      } else {
        final Page page =
        newPage(req, resp, session,
            "azkaban/viewer/hive/velocity/hive-browser.vm");
        page.render();
      }
    } catch (final Exception e) {
      throw new IllegalStateException("Error processing request: "
          + e.getMessage(), e);
    }
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    
  }

  private ResultSet getResultSet(String sqlStatement) throws SQLException {
  
    //Registering the Driver
    // DriverManager.registerDriver(new org.apache.derby.jdbc.ClientDriver());
    DriverManager.registerDriver(new org.apache.hive.jdbc.HiveDriver());
    Connection con = DriverManager.getConnection(viewerDbUrl, viewerDbUser, viewerDbPass);
    System.out.println("Connection established......");
    //Creating a Statement object
    Statement stmt = con.createStatement();
    //Retrieving the data
    ResultSet rs = stmt.executeQuery(sqlStatement);
    return rs;
    
  }

  private ResultSet getResultSet(String sqlStatement, String sql2) throws SQLException {
  
    //Registering the Driver
    // DriverManager.registerDriver(new org.apache.derby.jdbc.ClientDriver());
    DriverManager.registerDriver(new org.apache.hive.jdbc.HiveDriver());
    Connection con = DriverManager.getConnection(viewerDbUrl, viewerDbUser, viewerDbPass);
    System.out.println("Connection established......");
    //Creating a Statement object
    Statement stmt = con.createStatement();
    //Retrieving the data
    stmt.executeQuery(sqlStatement);
    ResultSet rs = stmt.executeQuery(sql2);
    return rs;
    
  }

  private ResultSetMetaData getResultSetMetaData(ResultSet rs) throws SQLException {
    return rs.getMetaData();
  }

}