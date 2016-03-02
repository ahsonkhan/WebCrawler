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
    private static ArrayList listOfLinks = new ArrayList();

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
        ExtractAndPushPlayerStatsData(rootUrl);
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

    private static boolean PushToDB(StringBuilder query) {
        if (conn == null) {
            return false;
        }

        PreparedStatement stmt;

        try {
            stmt = conn.prepareStatement(query.toString());
            stmt.executeUpdate();
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

        StringBuilder query = new StringBuilder();

        query.append("INSERT INTO NBAGame (gameID, homeTeamID, awayTeamID, date, season, homeScore, awayScore, winnerID) values");

        Map<String, Integer> teamIDs = LookUpAllTeamIDs();
        if (teamIDs == null) {
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

                if (!(cols.get(2).text().equals("Box Score"))) {
                    break;
                }

                Elements links = cols.get(0).select("a[href]");
                if (links.get(0).attr("href").contains("/boxscores/index.cgi")) {
                    String dateStr = cols.get(0).attr("csk");
                    dateStr = dateStr.replaceAll("[^\\d.]", "");
                    dateInt = Integer.parseInt(dateStr);
                    dateInt = dateInt / 10;
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
            if (pushToDB && !PushToDB(query, counter - 1)) {
                System.out.println("Failed to submit query to DB - ExtractAndPushGameData: counter = " + counter + ".");
            }
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

    private static Map<String, Integer> LookUpAllPlayerIDs() {
        if (conn == null) {
            return null;
        }

        String query = "Select playerName, playerID from NBAPlayer group by playerName;";
        PreparedStatement stmt;
        ResultSet rs;
        Map<String, Integer> playerIDs = new HashMap<>();

        try {
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            while (rs.next()) {
                playerIDs.put(rs.getString(1), rs.getInt(2));
            }
        } catch (Exception e) {
            System.out.println(e);
        }

        return playerIDs;
    }

    private static void GetLinks(String rootUrl) {
        Document doc;
        try {
            String season2016Url = rootUrl + "leagues/NBA_2016_games.html";
            doc = Jsoup.connect(season2016Url).get();
            Element games = doc.getElementById("games");
            Elements rows = games.select("tr");

            for (Element row : rows) {
                Elements cols = row.select("td");

                if (cols.size() == 0) {
                    continue;
                }

                if (cols.get(2).text().equals("Box Score")) {
                    Elements links = cols.get(2).select("a[href]");
                    listOfLinks.add(links.get(0).attr("href"));
                } else {
                    break;
                }
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static void ExtractAndPushPlayerStatsData(String rootUrl) {
        String separator;
        int counter = 1;
        String team1Name;
        String team2Name;
        int team1ID;
        int team2ID;
        Document doc;
        StringBuilder query;

        Map<String, Integer> teamIDs = LookUpAllTeamIDs();
        if (teamIDs == null) {
            return;
        }

        Map<String, Integer> playerIDs = LookUpAllPlayerIDs();
        if (playerIDs == null) {
            return;
        }

        Map<String, String> tableIDs = new HashMap<>();
        tableIDs.put("Brooklyn Nets", "BRK_basic");
        tableIDs.put("Charlotte Hornets", "CHO_basic");
        tableIDs.put("Oklahoma City Thunder", "OKC_basic");

        GetLinks(rootUrl);

        int count = 0;
        for (Object o : listOfLinks) {
            count++;
            System.out.println(count);
            query = new StringBuilder();
            query.append("INSERT INTO NBAPlayerStats (rowID, playerID, gameID, teamID, MP, FG, FGA, `FG%`, 3P, 3PA, `3P%`, FT, FTA, `FT%`, ORB, DRB, TRB, AST, STL, BLK, TOV, PF, PTS) values");
            separator = "";

            try {
                String gameLink = rootUrl.substring(0, rootUrl.length() - 1) + o.toString();
                doc = Jsoup.connect(gameLink).get();
                Elements teams = doc.select("h2");

                team1Name = teams.get(0).text();
                team1Name = team1Name.replace("-", "");
                team1Name = team1Name.replace("(", "");
                team1Name = team1Name.replace(")", "");
                team1Name = team1Name.replace(String.valueOf((char) 160), "").trim();
                team1Name = team1Name.replaceAll("\\d*$", "");
                team1Name = team1Name.trim();

                team2Name = teams.get(1).text();
                team2Name = team2Name.replace("-", "");
                team2Name = team2Name.replace("(", "");
                team2Name = team2Name.replace(")", "");
                team2Name = team2Name.replace(String.valueOf((char) 160), "").trim();
                team2Name = team2Name.replaceAll("\\d*$", "");
                team2Name = team2Name.trim();

                team1ID = teamIDs.get(team1Name);
                team2ID = teamIDs.get(team2Name);

                String team1TableID = team1Name.substring(0, 3).toUpperCase() + "_basic";
                String team2TableID = team2Name.substring(0, 3).toUpperCase() + "_basic";

                Element scores = doc.getElementById(team1TableID);
                if (scores == null) {
                    if (tableIDs.containsKey(team1Name)) {
                        team1TableID = tableIDs.get(team1Name);
                    } else {
                        String[] words = team1Name.split(" ");
                        team1TableID = "";
                        for (String word : words) {
                            team1TableID += word.substring(0, 1).toUpperCase();
                        }
                        team1TableID += "_basic";
                    }
                    scores = doc.getElementById(team1TableID);
                }
                System.out.println(team1TableID);
                Elements rows = scores.select("tr");

                for (Element row : rows) {
                    int playerID = 0;

                    Elements cols = row.select("td");

                    if (cols.size() == 0) {
                        continue;
                    }

                    if (cols.get(0).text().equals("Team Totals")) {
                        break;
                    }

                    Elements links = cols.get(0).select("a[href]");
                    String playerName = links.get(0).text();
                    playerID = playerIDs.get(playerName);

                    if (!(cols.get(1).text().matches(".*\\d.*"))) {
                        break;
                    }

                    int MP = cols.get(1).text().isEmpty() ? 0 : Integer.parseInt(cols.get(1).text().split(":")[0]);
                    int FG = cols.get(2).text().isEmpty() ? 0 : Integer.parseInt(cols.get(2).text());
                    int FGA = cols.get(3).text().isEmpty() ? 0 : Integer.parseInt(cols.get(3).text());
                    double FG_percent = cols.get(4).text().isEmpty() ? 0 : Double.parseDouble(cols.get(4).text());
                    int ThreeP = cols.get(5).text().isEmpty() ? 0 : Integer.parseInt(cols.get(5).text());
                    int ThreePA = cols.get(6).text().isEmpty() ? 0 : Integer.parseInt(cols.get(6).text());
                    double ThreeP_percent = cols.get(7).text().isEmpty() ? 0 : Double.parseDouble(cols.get(7).text());
                    int FT = cols.get(8).text().isEmpty() ? 0 : Integer.parseInt(cols.get(8).text());
                    int FTA = cols.get(9).text().isEmpty() ? 0 : Integer.parseInt(cols.get(9).text());
                    double FT_percent = cols.get(10).text().isEmpty() ? 0 : Double.parseDouble(cols.get(10).text());
                    int ORB = cols.get(11).text().isEmpty() ? 0 : Integer.parseInt(cols.get(11).text());
                    int DRB = cols.get(12).text().isEmpty() ? 0 : Integer.parseInt(cols.get(12).text());
                    int TRB = cols.get(13).text().isEmpty() ? 0 : Integer.parseInt(cols.get(13).text());
                    int AST = cols.get(14).text().isEmpty() ? 0 : Integer.parseInt(cols.get(14).text());
                    int STL = cols.get(15).text().isEmpty() ? 0 : Integer.parseInt(cols.get(15).text());
                    int BLK = cols.get(16).text().isEmpty() ? 0 : Integer.parseInt(cols.get(16).text());
                    int TOV = cols.get(17).text().isEmpty() ? 0 : Integer.parseInt(cols.get(17).text());
                    int PF = cols.get(18).text().isEmpty() ? 0 : Integer.parseInt(cols.get(18).text());
                    int PTS = cols.get(19).text().isEmpty() ? 0 : Integer.parseInt(cols.get(19).text());

                    query.append(separator);
                    separator = ",";
                    query.append(" (");
                    query.append(counter);
                    query.append(", ");
                    query.append(playerID);
                    query.append(", ");
                    query.append(count);
                    query.append(", ");
                    query.append(team1ID);
                    query.append(", ");
                    query.append(MP);
                    query.append(", ");
                    query.append(FG);
                    query.append(", ");
                    query.append(FGA);
                    query.append(", ");
                    query.append(FG_percent);
                    query.append(", ");
                    query.append(ThreeP);
                    query.append(", ");
                    query.append(ThreePA);
                    query.append(", ");
                    query.append(ThreeP_percent);
                    query.append(", ");
                    query.append(FT);
                    query.append(", ");
                    query.append(FTA);
                    query.append(", ");
                    query.append(FT_percent);
                    query.append(", ");
                    query.append(ORB);
                    query.append(", ");
                    query.append(DRB);
                    query.append(", ");
                    query.append(TRB);
                    query.append(", ");
                    query.append(AST);
                    query.append(", ");
                    query.append(STL);
                    query.append(", ");
                    query.append(BLK);
                    query.append(", ");
                    query.append(TOV);
                    query.append(", ");
                    query.append(PF);
                    query.append(", ");
                    query.append(PTS);
                    query.append(")");

                    counter++;
                }

                scores = doc.getElementById(team2TableID);
                if (scores == null) {
                    if (tableIDs.containsKey(team2Name)) {
                        team2TableID = tableIDs.get(team2Name);
                    } else {
                        String[] words = team2Name.split(" ");
                        team2TableID = "";
                        for (String word : words) {
                            team2TableID += word.substring(0, 1).toUpperCase();
                        }
                        team2TableID += "_basic";
                    }
                    scores = doc.getElementById(team2TableID);
                }
                System.out.println(team2TableID);
                rows = scores.select("tr");

                for (Element row : rows) {
                    int playerID = 0;

                    Elements cols = row.select("td");

                    if (cols.size() == 0) {
                        continue;
                    }

                    if (cols.get(0).text().equals("Team Totals")) {
                        break;
                    }

                    Elements links = cols.get(0).select("a[href]");
                    String playerName = links.get(0).text();
                    playerID = playerIDs.get(playerName);

                    if (!(cols.get(1).text().matches(".*\\d.*"))) {
                        break;
                    }

                    int MP = cols.get(1).text().isEmpty() ? 0 : Integer.parseInt(cols.get(1).text().split(":")[0]);
                    int FG = cols.get(2).text().isEmpty() ? 0 : Integer.parseInt(cols.get(2).text());
                    int FGA = cols.get(3).text().isEmpty() ? 0 : Integer.parseInt(cols.get(3).text());
                    double FG_percent = cols.get(4).text().isEmpty() ? 0 : Double.parseDouble(cols.get(4).text());
                    int ThreeP = cols.get(5).text().isEmpty() ? 0 : Integer.parseInt(cols.get(5).text());
                    int ThreePA = cols.get(6).text().isEmpty() ? 0 : Integer.parseInt(cols.get(6).text());
                    double ThreeP_percent = cols.get(7).text().isEmpty() ? 0 : Double.parseDouble(cols.get(7).text());
                    int FT = cols.get(8).text().isEmpty() ? 0 : Integer.parseInt(cols.get(8).text());
                    int FTA = cols.get(9).text().isEmpty() ? 0 : Integer.parseInt(cols.get(9).text());
                    double FT_percent = cols.get(10).text().isEmpty() ? 0 : Double.parseDouble(cols.get(10).text());
                    int ORB = cols.get(11).text().isEmpty() ? 0 : Integer.parseInt(cols.get(11).text());
                    int DRB = cols.get(12).text().isEmpty() ? 0 : Integer.parseInt(cols.get(12).text());
                    int TRB = cols.get(13).text().isEmpty() ? 0 : Integer.parseInt(cols.get(13).text());
                    int AST = cols.get(14).text().isEmpty() ? 0 : Integer.parseInt(cols.get(14).text());
                    int STL = cols.get(15).text().isEmpty() ? 0 : Integer.parseInt(cols.get(15).text());
                    int BLK = cols.get(16).text().isEmpty() ? 0 : Integer.parseInt(cols.get(16).text());
                    int TOV = cols.get(17).text().isEmpty() ? 0 : Integer.parseInt(cols.get(17).text());
                    int PF = cols.get(18).text().isEmpty() ? 0 : Integer.parseInt(cols.get(18).text());
                    int PTS = cols.get(19).text().isEmpty() ? 0 : Integer.parseInt(cols.get(19).text());

                    query.append(separator);
                    separator = ",";
                    query.append(" (");
                    query.append(counter);
                    query.append(", ");
                    query.append(playerID);
                    query.append(", ");
                    query.append(count);
                    query.append(", ");
                    query.append(team2ID);
                    query.append(", ");
                    query.append(MP);
                    query.append(", ");
                    query.append(FG);
                    query.append(", ");
                    query.append(FGA);
                    query.append(", ");
                    query.append(FG_percent);
                    query.append(", ");
                    query.append(ThreeP);
                    query.append(", ");
                    query.append(ThreePA);
                    query.append(", ");
                    query.append(ThreeP_percent);
                    query.append(", ");
                    query.append(FT);
                    query.append(", ");
                    query.append(FTA);
                    query.append(", ");
                    query.append(FT_percent);
                    query.append(", ");
                    query.append(ORB);
                    query.append(", ");
                    query.append(DRB);
                    query.append(", ");
                    query.append(TRB);
                    query.append(", ");
                    query.append(AST);
                    query.append(", ");
                    query.append(STL);
                    query.append(", ");
                    query.append(BLK);
                    query.append(", ");
                    query.append(TOV);
                    query.append(", ");
                    query.append(PF);
                    query.append(", ");
                    query.append(PTS);
                    query.append(")");

                    counter++;
                }
                query.append(";");
                if (!PushToDB(query)) {
                    System.out.println("Failed to submit query to DB - ExtractAndPushPlayerStatsData: counter = " + counter + ".");
                }
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        }
    }
}
