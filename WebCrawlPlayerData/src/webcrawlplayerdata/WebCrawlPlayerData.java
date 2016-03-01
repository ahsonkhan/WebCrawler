/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package webcrawlplayerdata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
//import java.util.logging.Level;
//import java.util.logging.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

/**
 *
 * @author AhsonAhmed
 */
public class WebCrawlPlayerData {

    private static Connection conn = null;
    private final static String RDSUrl = "jdbc:mysql://fantasysports.cvut0y8wktov.us-west-2.rds.amazonaws.com:3306/fantasysports?connectTimeout=2000";
    private final static String RDSUser = "root";
    private final static String RDSPassword = "hamyharrymic";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (!ConnectToDB() || conn == null) {
            return;
        }

        String rootUrl = "http://www.basketball-reference.com/";

        //ExtractAndPushPlayerData(rootUrl);
        //ExtractAndPushTeamData(rootUrl);
        //ExtractAndPushGameData(rootUrl, true);
        ExtractAndPushGameData(rootUrl, false);
    }

    private static boolean ConnectToDB() {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection(RDSUrl, RDSUser, RDSPassword);
        } catch (Exception e) {
            System.out.println(e);
            return false;
        }
        return true;
    }

    private static void ExtractAndPushPlayerData(String rootUrl) {
        String separator;
        int counter = 1;
        StringBuilder query;
        Document doc;

        String playerListUrl = rootUrl + "players/";

        for (int i = 0; i < 26; i++) {
            try {
                char alphabet = (char) (97 + i);
                String playerListAlphabet = playerListUrl + alphabet + "/";
                doc = Jsoup.connect(playerListAlphabet).get();
                Element players = doc.getElementById("players");
                Elements links = players.select("a[href]");

                Elements numPlayers = doc.select("h2");
                String numOfPlayers = numPlayers.get(0).text();
                numOfPlayers = numOfPlayers.replaceAll("[^\\d.]", "");
                int numPlayersInt = Integer.parseInt(numOfPlayers);

                query = new StringBuilder();
                query.append("INSERT INTO NBAPlayer (playerID, playerName) values");
                separator = "";

                for (Element link : links) {
                    String playerLink = link.attr("abs:href");
                    if (!playerLink.contains(playerListUrl)) {
                        continue;
                    }
                    query.append(separator);
                    separator = ",";
                    query.append(" (");
                    query.append(counter);
                    query.append(", \"");
                    query.append(link.text());
                    query.append("\")");
                    counter++;
                }

                query.append(";");

                if (!PushToDB(query, numPlayersInt)) {
                    System.out.println("Failed to submit query to DB: counter = " + counter + ", alphabet = " + alphabet + ".");
                }
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        }
    }
    
    private static boolean PushToDB(StringBuilder query, int verifyCount) {
        if (conn == null) {
            return false;
        }

        PreparedStatement stmt;

        try {
            stmt = conn.prepareStatement(query.toString());
            int rowCount = stmt.executeUpdate();
            if (verifyCount != rowCount) {
                System.out.println("Mistake in query: rowCount = " + rowCount + ", verifyCount = " + verifyCount + ".");
            }
        } catch (Exception e) {
            System.out.println(e);
            return false;
        }

        return true;
    }

    private static void ExtractAndPushTeamData(String rootUrl) {
        String separator = "";
        int counter = 1;
        int seasonID = 201516;
        Document doc;
        StringBuilder query = new StringBuilder();

        query.append("INSERT INTO NBATeam (teamID, teamName, seasonID, W, L, `WL%`, GB, PSG, PAG, SRS) values");

        try {
            String season2016Url = rootUrl + "leagues/NBA_2016.html";
            doc = Jsoup.connect(season2016Url).get();
            Element teamsE = doc.getElementById("E_standings");
            Element teamsW = doc.getElementById("W_standings");

            Elements rows = teamsE.select("tr");
            for (Element row : rows) {
                String teamName;
                int win;
                int loss;
                double wl_percent;
                double gamesBehind = 0;
                double psg;
                double pag;
                double srs;

                Elements cols = row.select("td");
                if (cols.size() == 0 || !(cols.get(0).text().contains("("))) {
                    continue;
                } else {
                    teamName = cols.get(0).text();
                    teamName = teamName.replace("*", "");
                    teamName = teamName.replace("(", "");
                    teamName = teamName.replace(")", "");
                    teamName = teamName.replace(String.valueOf((char) 160), "").trim();
                    teamName = teamName.replaceAll("\\d*$", "");
                    win = Integer.parseInt(cols.get(1).text());
                    loss = Integer.parseInt(cols.get(2).text());
                    wl_percent = Double.parseDouble(cols.get(3).text());
                    psg = Double.parseDouble(cols.get(5).text());
                    pag = Double.parseDouble(cols.get(6).text());
                    srs = Double.parseDouble(cols.get(7).text());
                }

                query.append(separator);
                separator = ",";
                query.append(" (");
                query.append(counter);
                query.append(", \"");
                query.append(teamName);
                query.append("\", ");
                query.append(seasonID);
                query.append(", ");
                query.append(win);
                query.append(", ");
                query.append(loss);
                query.append(", ");
                query.append(wl_percent);
                query.append(", ");
                query.append(gamesBehind);
                query.append(", ");
                query.append(psg);
                query.append(", ");
                query.append(pag);
                query.append(", ");
                query.append(srs);
                query.append(")");
                counter++;
            }

            rows = teamsW.select("tr");
            for (Element row : rows) {
                String teamName;
                int win;
                int loss;
                double wl_percent;
                double gamesBehind = 0;
                double psg;
                double pag;
                double srs;

                Elements cols = row.select("td");
                if (cols.size() == 0 || !(cols.get(0).text().contains("("))) {
                    continue;
                } else {
                    teamName = cols.get(0).text();
                    teamName = teamName.replace("*", "");
                    teamName = teamName.replace("(", "");
                    teamName = teamName.replace(")", "");
                    teamName = teamName.replace(String.valueOf((char) 160), "").trim();
                    teamName = teamName.replaceAll("\\d*$", "");
                    win = Integer.parseInt(cols.get(1).text());
                    loss = Integer.parseInt(cols.get(2).text());
                    wl_percent = Double.parseDouble(cols.get(3).text());
                    psg = Double.parseDouble(cols.get(5).text());
                    pag = Double.parseDouble(cols.get(6).text());
                    srs = Double.parseDouble(cols.get(7).text());
                }

                query.append(separator);
                separator = ",";
                query.append(" (");
                query.append(counter);
                query.append(", \"");
                query.append(teamName);
                query.append("\", ");
                query.append(seasonID);
                query.append(", ");
                query.append(win);
                query.append(", ");
                query.append(loss);
                query.append(", ");
                query.append(wl_percent);
                query.append(", ");
                query.append(gamesBehind);
                query.append(", ");
                query.append(psg);
                query.append(", ");
                query.append(pag);
                query.append(", ");
                query.append(srs);
                query.append(")");
                counter++;
            }
            
            query.append(";");
            if (!PushToDB(query, 30)) {
                System.out.println("Failed to submit query to DB - ExtractAndPushTeamData: counter = " + counter + ".");
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static void ExtractAndPushGameData(String rootUrl, boolean pushToDB) {
        String separator = "";
        int counter = 1;
        int seasonID = 201516;
        Document doc;
        ArrayList listOfLinks = new ArrayList();
        StringBuilder query = new StringBuilder();

        query.append("INSERT INTO NBAGame (gameID, homeTeamID, awayTeamID, date, season, homeScore, awayScore, winnerID) values");

        Map<String, Integer> teamIDs = LookUpAllTeamIDs();
        if (teamIDs == null)
        {
            return;
        }
        
        try {
            String season2016Url = rootUrl + "leagues/NBA_2016_games.html";
            doc = Jsoup.connect(season2016Url).get();
            Element games = doc.getElementById("games");
            Elements rows = games.select("tr");

            for (Element row : rows) {
                int dateInt = 0;
                String homeTeamName;
                String awayTeamName;
                int homeTeamID;
                int awayTeamID;
                int homeTeamScore;
                int awayTeamScore;
                String winnerTeamName;
                int winnerTeamID;

                Elements cols = row.select("td");
                
                if (cols.size() == 0) {
                    continue;
                }

                if (cols.get(2).text().equals("Box Score")) {
                    listOfLinks.add(cols.get(2).attr("href"));
                } else {
                    break;
                }

                Elements links = cols.get(0).select("a[href]");
                if (links.get(0).attr("href").contains("/boxscores/index.cgi")) {
                    String dateStr = cols.get(0).attr("csk");
                    dateStr = dateStr.replaceAll("[^\\d.]", "");
                    dateInt = Integer.parseInt(dateStr);
                    dateInt = dateInt/10;
                }
                awayTeamName = cols.get(3).text();
                awayTeamScore = Integer.parseInt(cols.get(4).text());

                homeTeamName = cols.get(5).text();
                homeTeamScore = Integer.parseInt(cols.get(6).text());

                if (awayTeamScore > homeTeamScore) {
                    winnerTeamName = awayTeamName;
                } else {
                    winnerTeamName = homeTeamName;
                }

                homeTeamID = teamIDs.get(homeTeamName);
                awayTeamID = teamIDs.get(awayTeamName);
                winnerTeamID = teamIDs.get(winnerTeamName);

                query.append(separator);
                separator = ",";
                query.append(" (");
                query.append(counter);
                query.append(", ");
                query.append(homeTeamID);
                query.append(", ");
                query.append(awayTeamID);
                query.append(", ");
                query.append(dateInt);
                query.append(", ");
                query.append(seasonID);
                query.append(", ");
                query.append(homeTeamScore);
                query.append(", ");
                query.append(awayTeamScore);
                query.append(", ");
                query.append(winnerTeamID);
                query.append(")");
                counter++;
            }

            query.append(";");
            if (pushToDB && !PushToDB(query, counter-1)) {
                System.out.println("Failed to submit query to DB - ExtractAndPushGameData: counter = " + counter + ".");
            }
            /*for (Object o : listOfLinks)
             {
             System.out.println(o.toString());
             }*/
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static Map<String, Integer> LookUpAllTeamIDs() {
        if (conn == null) {
            return null;
        }

        String query = "Select teamName, teamID from NBATeam";
        PreparedStatement stmt;
        ResultSet rs;
        Map<String, Integer> teamIDs = new HashMap<>();
        
        try {
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            while (rs.next()) {
                teamIDs.put(rs.getString(1), rs.getInt(2));
            }
        } catch (Exception e) {
            System.out.println(e);
        }

        return teamIDs;
    }
}
