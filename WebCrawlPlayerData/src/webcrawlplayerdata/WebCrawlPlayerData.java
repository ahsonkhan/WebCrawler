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

        ExtractAndPushPlayerData(rootUrl);
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

}
