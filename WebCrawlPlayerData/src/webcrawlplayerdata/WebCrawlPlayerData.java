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

/**
 *
 * @author AhsonAhmed
 */
public class WebCrawlPlayerData {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        String rootUrl = "http://www.basketball-reference.com/";
        
        int[] requestedStats = {10, 11, 20, 23, 24, 25, 26, 29};
        Map<Integer, String> statName = new HashMap<Integer, String>();
        statName.put(10, "FG%");
        statName.put(11, "3P");
        statName.put(20, "FT%");
        statName.put(23, "TRB");
        statName.put(24, "AST");
        statName.put(25, "STL");
        statName.put(26, "BLK");
        statName.put(29, "PTS");
        
        Document doc;
        try {
            for (int i = 0; i < 26; i++)
            {
                char alphabet = (char)(97 + i);
                String playerListUrl = rootUrl + "players/" + alphabet + "/";
                doc = Jsoup.connect(playerListUrl).get();
                Element players = doc.getElementById("players");
                Elements links = players.select("a[href]");
                
                for (Element link : links) {
                    String playerLink = link.attr("abs:href");
                    if (!playerLink.contains(playerListUrl)) continue;
                    //System.out.println(playerLink);
                    Document playerDoc = Jsoup.connect(playerLink).get();
                    //Element name = playerDoc.getElementsByClass("person_image_offset").first();
                    System.out.println(link.text());
                    Element totals = playerDoc.getElementById("totals");
                    Elements footer = totals.select("tfoot");
                    Element data = footer.get(0);
                    Element list = data.child(0);
                    Elements items = list.select("td");
                    
                    // TODO: Push to DB instead of console.
                    for (int statIndex : requestedStats)
                    {
                        System.out.print(statName.get(statIndex) + " - ");
                        System.out.println(items.get(statIndex).text());
                    }
                    
                    //System.out.println(alphabet);
                }
                
                //System.out.println(alphabet);
            }
            
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
            //Logger.getLogger(WebCrawlPlayerData.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
