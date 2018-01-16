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
        buildOUTrends();
        buildAWOUTrends();
        buildALOUTrends();
        createTodaysGamesView();
                    
            
        

        
        
        
    }   
    

    
    


    //TEAMS
    private static void buildTeamsData(String[] Teams) throws IOException, SQLException {
        
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
    private static void createTeamsTable() {
        
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
       String sql = "INSERT INTO teamData(teamname, streak) VALUES(?,?)";
 
        try (PreparedStatement pstmt = c.prepareStatement(sql)) {
            pstmt.setString(1, teamname);
            pstmt.setString(2, streak);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    private static String[] getTeams() {
        String[] Teams = {      
        "atlanta-hawks",
        "boston-celtics",
        "brooklyn-nets",
        "charlotte-hornets",
        "chicago-bulls",      
        "cleveland-cavaliers",
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

    //SCHEDULES
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
    private static void createTeamScheduleTable() {
        
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
    
    //ALL GAMES OU TRENDS
    private static void buildOUTrends() throws IOException, SQLException {
        String teamLink = "https://www.teamrankings.com/nba/trends/ou_trends/";
        
        createOUTrendsTable();
                        
            Document doc = Jsoup.connect(teamLink).get();
            
            Element table = doc.select(".tr-table.datatable.scrollable").get(0); //select the first table.
            Elements rows = table.select("tr");

            for (int j = 1; j < rows.size(); j++) { //first row is the col names so skip it.
                Element row = rows.get(j);
                Elements cols = row.select("td");
                Elements cols2 = row.select("td:eq(0) > a");
                Element link = cols2.first();
                String url = link.attr("href").replace("https://www.teamrankings.com/nba/team/", "");
                
//                System.out.println(cols.get(0).text());
//                System.out.println(cols.get(1).text());
//                System.out.println(cols.get(2).text());
//                System.out.println(cols.get(3).text());
                
                insertOUTrends(url,cols.get(1).text(),cols.get(2).text(),cols.get(3).text());
            
        }
    }
    private static void insertOUTrends(String teamname, String overrecord, String overpercent, String underpercent) throws IOException, SQLException {
       String sql = "INSERT INTO teamOUTrends(teamname, overrecord, overpercent, underpercent) VALUES(?,?,?,?)";
       
       try(PreparedStatement pstmt = c.prepareStatement(sql)) {
            pstmt.setString(1, teamname);
            pstmt.setString(2, overrecord);
            pstmt.setString(3, overpercent);
            pstmt.setString(4, underpercent);
            pstmt.executeUpdate();
       } catch (SQLException e) {
            System.out.println(e.getMessage());
       }       
        
    }
    private static void createOUTrendsTable() {
        
        // SQL statement for creating a new table
        String sql = "CREATE TABLE IF NOT EXISTS teamOUTrends (\n"
                + "	id integer PRIMARY KEY,\n"
                + "	teamname text NOT NULL,\n"
                + "	overrecord text NOT NULL,\n"
                + "	overpercent text NOT NULL,\n"
                + "	underpercent text NOT NULL\n"
                + ");";
        
        try (Statement stmt = c.createStatement()) {
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    //AFTER WIN GAMES OU TRENDS
    private static void buildAWOUTrends() throws IOException, SQLException {
        String teamLink = "https://www.teamrankings.com/nba/trends/ou_trends/?sc=is_after_win";
        
        createAWOUTrendsTable();
                        
            Document doc = Jsoup.connect(teamLink).get();
            
            Element table = doc.select(".tr-table.datatable.scrollable").get(0); //select the first table.
            Elements rows = table.select("tr");

            for (int j = 1; j < rows.size(); j++) { //first row is the col names so skip it.
                Element row = rows.get(j);
                Elements cols = row.select("td");
                Elements cols2 = row.select("td:eq(0) > a");
                Element link = cols2.first();
                String url = link.attr("href").replace("https://www.teamrankings.com/nba/team/", "");
                
//                System.out.println(cols.get(0).text());
//                System.out.println(cols.get(1).text());
//                System.out.println(cols.get(2).text());
//                System.out.println(cols.get(3).text());
                
                insertAWOUTrends(url,cols.get(1).text(),cols.get(2).text(),cols.get(3).text());
            
        }
    }
    private static void insertAWOUTrends(String teamname, String overrecord, String overpercent, String underpercent) throws IOException, SQLException {
       String sql = "INSERT INTO teamAWOUTrends(teamname, overrecord, overpercent, underpercent) VALUES(?,?,?,?)";
       
       try(PreparedStatement pstmt = c.prepareStatement(sql)) {
            pstmt.setString(1, teamname);
            pstmt.setString(2, overrecord);
            pstmt.setString(3, overpercent);
            pstmt.setString(4, underpercent);
            pstmt.executeUpdate();
       } catch (SQLException e) {
            System.out.println(e.getMessage());
       }       
        
    }
    private static void createAWOUTrendsTable() {
        
        // SQL statement for creating a new table
        String sql = "CREATE TABLE IF NOT EXISTS teamAWOUTrends (\n"
                + "	id integer PRIMARY KEY,\n"
                + "	teamname text NOT NULL,\n"
                + "	overrecord text NOT NULL,\n"
                + "	overpercent text NOT NULL,\n"
                + "	underpercent text NOT NULL\n"
                + ");";
        
        try (Statement stmt = c.createStatement()) {
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    
    //AFTER WIN GAMES OU TRENDS
    private static void buildALOUTrends() throws IOException, SQLException {
        String teamLink = "https://www.teamrankings.com/nba/trends/ou_trends/?sc=is_after_loss";
        
        createALOUTrendsTable();
                        
            Document doc = Jsoup.connect(teamLink).get();
            
            Element table = doc.select(".tr-table.datatable.scrollable").get(0); //select the first table.
            Elements rows = table.select("tr");

            for (int j = 1; j < rows.size(); j++) { //first row is the col names so skip it.
                Element row = rows.get(j);
                Elements cols = row.select("td");
                Elements cols2 = row.select("td:eq(0) > a");
                Element link = cols2.first();
                String url = link.attr("href").replace("https://www.teamrankings.com/nba/team/", "");
                
//                System.out.println(cols.get(0).text());
//                System.out.println(cols.get(1).text());
//                System.out.println(cols.get(2).text());
//                System.out.println(cols.get(3).text());
                
                insertALOUTrends(url,cols.get(1).text(),cols.get(2).text(),cols.get(3).text());
            
        }
    }
    private static void insertALOUTrends(String teamname, String overrecord, String overpercent, String underpercent) throws IOException, SQLException {
       String sql = "INSERT INTO teamALOUTrends(teamname, overrecord, overpercent, underpercent) VALUES(?,?,?,?)";
       
       try(PreparedStatement pstmt = c.prepareStatement(sql)) {
            pstmt.setString(1, teamname);
            pstmt.setString(2, overrecord);
            pstmt.setString(3, overpercent);
            pstmt.setString(4, underpercent);
            pstmt.executeUpdate();
       } catch (SQLException e) {
            System.out.println(e.getMessage());
       }       
        
    }
    private static void createALOUTrendsTable() {
        
        // SQL statement for creating a new table
        String sql = "CREATE TABLE IF NOT EXISTS teamALOUTrends (\n"
                + "	id integer PRIMARY KEY,\n"
                + "	teamname text NOT NULL,\n"
                + "	overrecord text NOT NULL,\n"
                + "	overpercent text NOT NULL,\n"
                + "	underpercent text NOT NULL\n"
                + ");";
        
        try (Statement stmt = c.createStatement()) {
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    //VIEWS
    private static void createTodaysGamesView() {
        
        // SQL statement for creating a new table
        String sql = "CREATE VIEW \"v_TodaysGames\" AS \n"
                + "	select ts.*, td1.streak as TeamWinStreak, td2.streak as OppWinStreak\n"
                + "	from teamSchedule ts \n"
                + "	join teamData td1 on \n"
                + "	ts.teamname = td1.teamname \n"
                + "	join teamData td2 on \n"
                + "	ts.opponent = td2.teamname \n"
                + "	where ts.gameDate = '01/13' \n"
                + "	order by ts.teamname\n"
                + ";";
        
        try (Statement stmt = c.createStatement()) {
            // create a new table
            stmt.execute(sql);
            System.out.println("todays games view created");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    //CONNECTIONS
    private static Connection c = null;
    public static Connection getConn() throws Exception {
        if(c == null){
        Class.forName("org.sqlite.JDBC");
        //c = DriverManager.getConnection("jdbc:sqlite:C:/sqlite/db/nba1.db");
        c = DriverManager.getConnection("jdbc:sqlite:/home/manderson/JavaProjects/nba_scrape/nba2.db");
        }
        return c;
    }
}
