/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package webscraper;

import java.io.IOException;
import org.jsoup.Jsoup;
import org.jsoup.helper.Validate;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlForm;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlSubmitInput;
import com.gargoylesoftware.htmlunit.html.HtmlDivision;

/**
 *
 * @author Mike
 */
public class WebScraper {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, Exception {
        // TODO code application logic here
        
        getConn();
        buildTeamsData(getTeams());
        buildScheduleData(getTeams());

        //https://www.teamrankings.com/nba/trends/ou_trends/
        
 
    }   
    

    
    

    private static void createTeamsDb() throws SQLException {
        String url = "jdbc:sqlite:C:/sqlite/db/nba.db";
            if (c != null) {
                DatabaseMetaData meta = c.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }

    }

    private static void createTeamsTable() {
       String url = "jdbc:sqlite:C:/sqlite/db/nba.db";
        
        // SQL statement for creating a new table
        String sql = "CREATE TABLE IF NOT EXISTS teamData (\n"
                + "	id integer PRIMARY KEY,\n"
                + "	teamname text NOT NULL,\n"
                + "	streak text NOT NULL\n"
                + ");";
        
        try (Statement stmt = c.createStatement()) {
            // create a new table
            stmt.execute(sql);
            System.out.println("teams table created");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void insertTeams(String teamname, String streak) {
       String url = "jdbc:sqlite:C:/sqlite/db/nba.db";
       String sql = "INSERT INTO teamData(teamname, streak) VALUES(?,?)";
 
        try (PreparedStatement pstmt = c.prepareStatement(sql)) {
            pstmt.setString(1, teamname);
            pstmt.setString(2, streak);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    private static void buildTeamsData(String[] Teams) throws IOException, SQLException {
        
        createTeamsDb();
        createTeamsTable();     
        
        for(int i=0; i < Teams.length; i++) {
            
            String teamLink = "https://www.teamrankings.com/nba/team/" + Teams[i] + "/";
            
            Document doc = Jsoup.connect(teamLink).get();
            Elements streaks = doc.select("tr.team-blockup-data > td:gt(1)"); 

            for (Element streak : streaks) {
            
                insertTeams(Teams[i], streak.text());
                //System.out.println(Teams[i] + " - " + streak.text());
            
            }
        }
    }

    private static void buildScheduleData(String[] Teams) throws IOException, SQLException {
        
        createTeamScheduleTable();

        for(int i=0; i < Teams.length; i++) {
            
            String teamLink = "https://www.teamrankings.com/nba/team/" + Teams[i] + "/";
            //String teamLink = "https://www.teamrankings.com/nba/team/cleveland-cavaliers";
                        
            Document doc = Jsoup.connect(teamLink).get();
            
            Element table = doc.select(".tr-table.datatable.scrollable").get(0); //select the first table.
            Elements rows = table.select("tr");

            for (int j = 1; j < rows.size(); j++) { //first row is the col names so skip it.
                Element row = rows.get(j);
                Elements cols = row.select("td");
                Elements cols2 = row.select("td:eq(1) > a");
                Element link = cols2.first();
                String url = link.attr("href").replace("/nba/team/", "");
                
                //gamedate, teamname, opponent, result, spread, total, money
                insertShedules(cols.get(0).text(), Teams[i], url, cols.get(2).text(), cols.get(6).text(), cols.get(7).text(), cols.get(8).text());
                
            }
        }
}

    private static String[] getTeams() {
        String[] Teams = {            
        "cleveland-cavaliers",
        "indiana-pacers",
        "atlanta-hawks",
        "boston-celtics",
        "brooklyn-nets",
        "charlotte-hornets",
        "chicago-bulls",
        "dallas-mavericks",
        "denver-nuggets",
        "detroit-pistons",
        "golden-state-warriors",
        "houston-rockets",
        "indiana-pacers",
        "los-angeles-clippers",
        "los-angeles-lakers",
        "memphis-grizzlies",
        "miami-heat",
        "milwaukee-bucks",
        "minnesota-timberwolves",
        "new-orleans-pelicans",
        "new-york-knicks",
        "oklahoma-city-thunder",
        "orlando-magic",
        "philadelphia-76ers",
        "phoenix-suns",
        "portland-trail-blazers",
        "sacramento-kings",
        "san-antonio-spurs",
        "toronto-raptors",
        "utah-jazz",
        "washington-wizards"       
        };
        
        return Teams;
        
    }

    private static void createTeamScheduleTable() {
        String url = "jdbc:sqlite:C:/sqlite/db/nba.db";
        
        // SQL statement for creating a new table
        String sql = "CREATE TABLE IF NOT EXISTS teamSchedule (\n"
                + "	id integer PRIMARY KEY,\n"
                + "	teamname text NOT NULL,\n"
                + "	opponent text NOT NULL, \n"
                + "	gamedate text NOT NULL, \n"
                + "	result text NOT NULL, \n"
                + "	spread text NOT NULL, \n"
                + "	total text NOT NULL, \n"
                + "	moneyline text NOT NULL\n"
                + ");";
        
        try (Statement stmt = c.createStatement()) {
            // create a new table
            stmt.execute(sql);
            System.out.println("team schedule table created");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void insertShedules(String gamedate, String teamname, String opponent, String result, String spread, String total, String moneyline) throws IOException, SQLException {
       String url = "jdbc:sqlite:C:/sqlite/db/nba.db";
       String sql = "INSERT INTO teamSchedule(gamedate, teamname, opponent, result, spread, total, moneyline) VALUES(?,?,?,?,?,?,?)";
 
       //Connection conn = DriverManager.getConnection(url);
       //PreparedStatement pstmt = conn.prepareStatement(sql);
       
       //try (Connection conn = DriverManager.getConnection(url);
       
       try(PreparedStatement pstmt = c.prepareStatement(sql)) {
            pstmt.setString(1, gamedate);
            pstmt.setString(2, teamname);
            pstmt.setString(3, opponent);
            pstmt.setString(4, result);
            pstmt.setString(5, spread);
            pstmt.setString(6, total);
            pstmt.setString(7, moneyline);
            pstmt.executeUpdate();
       } catch (SQLException e) {
            System.out.println(e.getMessage());
       }       
        
    }
    
    private static Connection c = null;
    public static Connection getConn() throws Exception {
        if(c == null){
        Class.forName("org.sqlite.JDBC");
        c = DriverManager.getConnection("jdbc:sqlite:C:/sqlite/db/nba.db");
        }
        return c;
    }

}
