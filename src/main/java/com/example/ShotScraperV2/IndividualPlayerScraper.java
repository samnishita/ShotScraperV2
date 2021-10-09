package com.example.ShotScraperV2;

import com.example.ShotScraperV2.nbaobjects.Player;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Scraper of all players
 */
public class IndividualPlayerScraper implements ScraperUtilsInterface {
    private final Logger LOGGER = LoggerFactory.getLogger(IndividualPlayerScraper.class);
    private String schema1, schema2;

    public IndividualPlayerScraper(String schema1, String schema2) {
        this.schema1 = schema1;
        this.schema2 = schema2;
        LOGGER.info("Initialized PlayerInfoFinder for " + schema1 + " and " + schema2);
    }

    /**
     * Scrapes all active years for a given player
     *
     * @param connPlayers1 connection to first player database
     * @param connPlayers2 connection to second player database
     */
    public void getPlayerActiveYears(Connection connPlayers1, Connection connPlayers2) throws InterruptedException {
        //Retired players who have no active years
        ArrayList<String> excludedIDs = new ArrayList<>(Arrays.asList("41", "986", "202070", "201195", "202221", "201998", "201987",
                "202364", "202067", "202238", "1626122", "202358", "202392", "1629129", "202375"));
        //These players may make a comeback but it's unlikely
        ArrayList<String> tempSkipIds = new ArrayList<>(Arrays.asList("1630258", "1629341", "1629624", "1630209", "1630492"));
        while (true) {
            //Get the player to scrape data
            Player polledPlayer = RunHandler.pollQueue();
            String playerTableName;
            //Exits while loop when polling returns null
            if (polledPlayer == null) {
                break;
            } else {
                String eachID = polledPlayer.getPlayerId() + "";
                String lastNameOrig = polledPlayer.getLastName();
                String firstNameOrig = polledPlayer.getFirstName();
                String lastName = lastNameOrig.replaceAll("[^A-Za-z0-9]", "");
                String firstName = firstNameOrig.replaceAll("[^A-Za-z0-9]", "");
                //Questionable call here
                if (excludedIDs.contains(eachID) || tempSkipIds.contains(eachID) || (polledPlayer.getFirstActiveYear().equals("2021-22") && polledPlayer.getFirstActiveYear().equals(polledPlayer.getMostRecentActiveYear()))) {
                    continue;
                }
                playerTableName = lastName + "_" + firstName + "_" + eachID + "_individual_data";
                try {
                    HashMap<String, ArrayList<Integer>> yearSeasonActivityMap = new HashMap<>();
                    //Fetch and parse the active seasons data
                    loopSearchIfError(eachID, firstName, lastName, yearSeasonActivityMap);
                    //Doesn't create table unless the player has been active once
                    //Shots are only available after 1996-97
                    if (yearSeasonActivityMap.keySet().size() > 0 && Integer.parseInt(Collections.max(yearSeasonActivityMap.keySet()).substring(0, 4)) >= 1996) {
                        createIndividualDataTable(playerTableName, connPlayers1, connPlayers2);
                        insertYears(yearSeasonActivityMap, playerTableName, connPlayers1, connPlayers2);
                    } else {
                        LOGGER.info("NO YEARS FOR " + firstName + " " + lastName + " (" + eachID + ")");
                    }
                } catch (Exception ex) {
                    LOGGER.error(ex.getMessage());
                }
            }
            //Pause to prevent server denying request
            if (RunHandler.peekQueue() != null) {
                Thread.sleep((long) (Math.random() * 20000));
            }
        }
    }

    /**
     * Records player active years and activity status in each season type for each active year
     *
     * @param text                  the response as JSON
     * @param sb                    the StringBuilder that builds the logging output
     * @param yearSeasonActivityMap map with (K,V) of (year a player is active, array of player activity status in each season type)
     */
    protected void recordSeasons(JSONObject text, StringBuilder sb, HashMap<String, ArrayList<Integer>> yearSeasonActivityMap) {
        //[Regular Season, Preseason, Playoffs]

        //Response has these keys to denote active seasons - look for these when parsing
        String[] desiredSearchStrings = new String[]{"SeasonTotalsRegularSeason", "SeasonTotalsPreseason", "SeasonTotalsPostSeason"};
        String[] loggingDescriptors = new String[]{"Regular Seasons", "PreSeasons", "PostSeasons"};
        int[] indexes = new int[]{0, 8, 2};
        int[] seasonCounter = new int[]{0, 0, 0};
        JSONArray rowSets;
        String year;
        for (int i = 0; i < 3; i++) {
            //First year that shots are available (2005-06 for preseason, 1996-97 for regular season and playoffs)
            int firstYear = desiredSearchStrings[i].equals("SeasonTotalsPreseason") ? 2005 : 1996;
            //Find key representing a season type
            String seasonType = text.getJSONArray("resultSets").getJSONObject(indexes[i]).getString("name");
            if (seasonType.equals(desiredSearchStrings[i])) {
                //Get all active years for found season type, as a JSON array
                rowSets = text.getJSONArray("resultSets").getJSONObject(indexes[i]).getJSONArray("rowSet");
                if (rowSets.length() != 0) {
                    //Iterate through all JSON arrays
                    for (int index = 0; index < rowSets.length(); index++) {
                        year = rowSets.getJSONArray(index).getString(1);
                        //Year must have available shots
                        if (Integer.parseInt(year.substring(0, 4)) >= firstYear) {
                            //Initialize array as map value if key does not exist
                            //For activity array: -1 is inactive, 1 is active
                            if (!yearSeasonActivityMap.containsKey(year)) {
                                ArrayList<Integer> activeSeasons = new ArrayList<>(Arrays.asList(-1, -1, -1));
                                activeSeasons.set(i, 1);
                                yearSeasonActivityMap.put(year, activeSeasons);
                            } else {
                                yearSeasonActivityMap.get(year).set(i, 1);
                            }
                            //Count active years for each season type for logging purposes
                            seasonCounter[i]++;
                        }
                    }
                }
            }
        }
        //For logging
        for (int i = 0; i < 3; i++) {
            sb.append("(").append(loggingDescriptors[i]).append(" : ").append(seasonCounter[i]).append(") \n");
        }

    }

    /**
     * Inserts active year and season type activity data to each player's personal table
     *
     * @param playerTableName the player's table name
     * @param year            the year to be inserted
     * @param connPlayers1    connection to first player database
     * @param connPlayers2    connection to second player database
     * @param seasonActivity  array of season activity for the given year
     */
    protected void insertGivenYear(String playerTableName, String year, Connection connPlayers1, Connection connPlayers2, ArrayList<Integer> seasonActivity) {
        try {
            String sqlInsert = "INSERT INTO " + playerTableName + " VALUES (?,?,?,?)";
            PreparedStatement stmt = connPlayers1.prepareStatement(sqlInsert);
            stmt.setString(1, year);
            stmt.setInt(2, seasonActivity.get(0));
            stmt.setInt(3, seasonActivity.get(1));
            stmt.setInt(4, seasonActivity.get(2));
            stmt.execute();
            if (connPlayers1 != connPlayers2) {
                stmt = connPlayers2.prepareStatement(sqlInsert);
                stmt.setString(1, year);
                stmt.setInt(2, seasonActivity.get(0));
                stmt.setInt(3, seasonActivity.get(1));
                stmt.setInt(4, seasonActivity.get(2));
                stmt.execute();
            }
            LOGGER.info("Inserting " + playerTableName + " : " + year);
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    /**
     * Creates each player's personal data table
     *
     * @param playerTableName the table name
     * @param connPlayers1    connection to first player database
     * @param connPlayers2    connection to second player database
     * @throws SQLException If the table creation fails
     */
    protected void createIndividualDataTable(String playerTableName, Connection connPlayers1, Connection connPlayers2) throws SQLException {
        String createTable = "CREATE TABLE IF NOT EXISTS `" + playerTableName + "` (\n"
                + "  `year` varchar(10) DEFAULT NULL,\n"
                + "  `reg` int DEFAULT '-1',\n"
                + "  `preseason` int DEFAULT '-1',\n"
                + "  `playoffs` int DEFAULT '-1',"
                + "  UNIQUE KEY `Year_UNIQUE` (`year`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
        connPlayers1.prepareStatement(createTable).execute();
        connPlayers2.prepareStatement(createTable).execute();
    }

    /**
     * Updates the player's activity in their personal data table for a given season type
     *
     * @param playerTableName the table name
     * @param year            the season
     * @param seasonType      the season type
     * @param connPlayers1    connection to first player database
     * @param connPlayers2    connection to second player database
     */
    protected void updateGivenSeasons(String playerTableName, String year, String seasonType, Connection connPlayers1, Connection connPlayers2) {
        try {
            String sqlUpdate = "UPDATE " + playerTableName + " SET " + seasonType + "= 1 WHERE year = \"" + year + "\"";
            connPlayers1.prepareStatement(sqlUpdate).execute();
            if (connPlayers1 != connPlayers2) {
                connPlayers2.prepareStatement(sqlUpdate).execute();
            }
            LOGGER.info("Updating " + playerTableName + " : " + year + " " + seasonType + " -> 1");
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    /**
     * Organizes player's active years by season type and inserts them into the given player table when not already present
     *
     * @param yearSeasonActivityMap map with (K,V) of (year a player is active, array of player activity status in each season type)
     * @param playerTableName       player's table name
     * @param connPlayers1          connection to first player database
     * @param connPlayers2          connection to second player database
     */
    private void insertYears(HashMap<String, ArrayList<Integer>> yearSeasonActivityMap, String playerTableName, Connection connPlayers1, Connection connPlayers2) {
        String[] parameterNames = new String[]{"reg", "preseason", "playoffs"};
        HashMap<String, ArrayList<Integer>> knownYearSeasonActivityMap = new HashMap<>();
        //Check for existing entries in database to reduce database queries
        try {
            ResultSet allYearData = connPlayers1.prepareStatement("SELECT * FROM " + playerTableName).executeQuery();
            while (allYearData.next()) {
                String eachYear = allYearData.getString("year");
                ArrayList<Integer> eachYearActivity = new ArrayList<>(Arrays.asList(-1, -1, -1));
                //Set array with database values (either 1 or -1)
                eachYearActivity.set(0, allYearData.getInt("reg"));
                eachYearActivity.set(1, allYearData.getInt("playoffs"));
                eachYearActivity.set(2, allYearData.getInt("preseason"));
                knownYearSeasonActivityMap.put(eachYear, eachYearActivity);
            }
            allYearData.close();
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
        //Update each season type activity status as needed if year already exists in database
        for (String yearKey : yearSeasonActivityMap.keySet()) {
            if (knownYearSeasonActivityMap.containsKey(yearKey)) {
                for (int i = 0; i < 3; i++) {
                    if (yearSeasonActivityMap.get(yearKey).get(i) == 1 && knownYearSeasonActivityMap.get(yearKey).get(i) == -1) {
                        updateGivenSeasons(playerTableName, yearKey, parameterNames[i], connPlayers1, connPlayers2);
                    }
                }
            } else {
                //Insert each year if it is not already present in table
                insertGivenYear(playerTableName, yearKey, connPlayers1, connPlayers2, yearSeasonActivityMap.get(yearKey));
            }
        }
    }

    /**
     * Retries searching for player data if original search times out
     *
     * @param eachID    player ID
     * @param firstName player first name
     * @param lastName  player last name
     */
    private void loopSearchIfError(String eachID, String firstName, String lastName, HashMap<String, ArrayList<Integer>> yearSeasonActivityMap) throws TimeoutException {
        int exceptionRetryCounter = 0;
        while (exceptionRetryCounter < 3) {
            try {
                String response = ScraperUtilsInterface.super.fetchSpecificURL("https://stats.nba.com/stats/playerprofilev2?LeagueID=00&PerMode=PerGame&PlayerID=" + eachID);
                StringBuilder sb = new StringBuilder(eachID + ": ");
                recordSeasons(new JSONObject(response), sb, yearSeasonActivityMap);
                LOGGER.info(sb.toString());
                exceptionRetryCounter = 5;
            } catch (InterruptedException | IOException ex) {
                exceptionRetryCounter++;
                LOGGER.error("Timeout caught in search for " + firstName + " " + lastName + ", Retrying (" + exceptionRetryCounter + ")");
            }
        }
        if (exceptionRetryCounter == 3) {
            LOGGER.error("Timeout caught in search for " + firstName + " " + lastName + ", Skipping");
        }
    }
}
