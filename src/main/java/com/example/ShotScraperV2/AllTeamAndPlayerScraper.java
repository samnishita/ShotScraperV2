package com.example.ShotScraperV2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Scraper for basic NBA team and player data
 */
@Component
public class AllTeamAndPlayerScraper implements ScraperUtilsInterface {
    private final Logger LOGGER = LoggerFactory.getLogger(AllTeamAndPlayerScraper.class);
    /**
     * Database schema names
     */
    private String schema1, schema2;

    /**
     * Establishes connections to databases
     *
     * @param schema1 name of first database schema
     * @param schema2 name of second database schema
     */
    @Autowired
    public AllTeamAndPlayerScraper(@Value("${playerschema1}") String schema1, @Value("${playerschema2}") String schema2) {
        this.schema1 = schema1;
        this.schema2 = schema2;
    }

    /**
     * Scrapes all basic team data and all basic player data
     *
     * @throws InterruptedException If fetching the site is interrupted
     * @throws IOException          If fetching the site is interrupted
     * @throws SQLException         If inserting the data into a database fails
     */
    public void getTeamAndPlayerData(Connection connPlayers1, Connection connPlayers2) throws InterruptedException, IOException, SQLException {
        createGeneralTablesIfNecessary(connPlayers1, schema1);
        createGeneralTablesIfNecessary(connPlayers2, schema2);
        //Record updated players, activity status, and active years for logging the results
        HashMap<String, Integer> updatedPlayerActivities = new HashMap<>();
        HashMap<String, Integer> updatedLatestActiveYears = new HashMap<>();
        String response = ScraperUtilsInterface.super.fetchSpecificURL("https://www.nba.com/stats/js/data/ptsd/stats_ptsd.js");
        //Parse response
        String[] splitResponse = response.split("\"teams\"")[1].split("\"players\"");
        String[] teams = splitResponse[0].split("\\]\\]");
        String[] players = splitResponse[1].split("\\]");
        //Parse the team data
        processTeamData(teams, connPlayers1, connPlayers2);
        //Parse the player data
        processPlayerData(players, updatedPlayerActivities, updatedLatestActiveYears, connPlayers1, connPlayers2);
        //Log results
        logScrapingResults(updatedPlayerActivities, updatedLatestActiveYears);
    }

    /**
     * Creates general use tables if they are not present in the connected database
     * <p>
     * The misc table is initialized with specific fields
     *
     * @param conn        the connection to the desired database
     * @param schemaAlias the alias of the database schema
     * @throws SQLException If table creation fails
     */
    protected void createGeneralTablesIfNecessary(Connection conn, String schemaAlias) throws SQLException {
        HashSet<String> allPlayerTables = new HashSet<>();
        String sqlSelect = "SELECT table_name FROM information_schema.tables";
        if (!schemaAlias.equals("")) {
            sqlSelect = sqlSelect + " WHERE table_schema = '" + ScraperUtilsInterface.super.getSchemaName(schemaAlias) + "'";
        }
        //Get all tables and create if tables don't exist
        ResultSet allTablesRS = conn.prepareStatement(sqlSelect).executeQuery();
        while (allTablesRS.next()) {
            allPlayerTables.add(allTablesRS.getString("table_name"));
        }
        allTablesRS.close();
        if (!allPlayerTables.contains("player_all_data")) {
            createLargePlayerTable("player_all_data", conn);
        }
        if (!allPlayerTables.contains("player_relevant_data")) {
            createLargePlayerTable("player_relevant_data", conn);
        }
        if (!allPlayerTables.contains("team_data")) {
            String createTeamTable = "CREATE TABLE IF NOT EXISTS `team_data` (\n"
                    + "  `id` int NOT NULL,\n"
                    + "  `abbr` varchar(20) DEFAULT NULL,\n"
                    + "  `casualname` varchar(45) DEFAULT NULL,\n"
                    + "  `firstname` varchar(45) DEFAULT NULL,\n"
                    + "  `secondname` varchar(45) DEFAULT NULL,\n"
                    + "  PRIMARY KEY (`id`),\n"
                    + "  UNIQUE KEY `id_UNIQUE` (`id`)\n"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
            conn.prepareStatement(createTeamTable).execute();
        }
        if (!allPlayerTables.contains("misc")) {
            String sql = "CREATE TABLE IF NOT EXISTS `misc` (\n"
                    + "`type` varchar(20) NOT NULL,\n"
                    + "`value` varchar(200) DEFAULT NULL,\n"
                    + "UNIQUE KEY `type_UNIQUE` (`type`)\n"
                    + ")\n"
                    + "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";
            conn.prepareStatement(sql).execute();
            //Initialize misc with parameter types
            try {
                String sqlInsert = "INSERT INTO misc (type,value) VALUES ('version',null),('announcement',null),('lastshotsadded',null),('current year', null)";
                conn.prepareStatement(sqlInsert).execute();
            } catch (Exception ex) {
                LOGGER.error(ex.getMessage());
            }
        }
    }

    /**
     * Creates a player table for players and their basic information and activity data
     *
     * @param tableName the name of the table
     * @param conn      connection to database
     * @throws SQLException If SQL query fails
     */
    private void createLargePlayerTable(String tableName, Connection conn) throws SQLException {
        String sqlCT = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
                + "  `id` int NOT NULL,\n"
                + "  `lastname` varchar(25) NOT NULL,\n"
                + "  `firstname` varchar(25) DEFAULT NULL,\n"
                + "  `firstactiveyear` varchar(25) DEFAULT NULL,\n"
                + "  `mostrecentactiveyear` varchar(25) DEFAULT NULL,\n"
                + "  `currentlyactive` tinyint DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  UNIQUE KEY `id_UNIQUE` (`id`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
        conn.prepareStatement(sqlCT).execute();
    }

    /**
     * Gets all entries for column 'id' for given SQL query
     *
     * @param sql query to be executed
     * @return hashSet of all IDs
     * @throws SQLException If the query fails
     */
    private HashSet<Integer> getAllIds(String sql, Connection connPlayers1, Connection connPlayers2) throws SQLException {
        ResultSet resultSet1 = connPlayers1.prepareStatement(sql).executeQuery();
        ResultSet resultSet2 = connPlayers2.prepareStatement(sql).executeQuery();
        HashSet<Integer> uniqueIds = new HashSet<>();
        while (resultSet1.next()) {
            uniqueIds.add(resultSet1.getInt("id"));
        }
        while (resultSet2.next()) {
            uniqueIds.add(resultSet2.getInt("id"));
        }
        resultSet1.close();
        resultSet2.close();
        return uniqueIds;
    }

    /**
     * Inserts basic player data and basic activity information into database
     *
     * @param insertPlayer  the SQL statement, formatted for a prepared statement
     * @param playerDetails array of player data scraped from site
     * @param conn          connection to database
     * @param activityIndex array index of data that indicates whether the player is active. Some players have more or fewer than two parts to their name. Active players have an additional parameter of their current team
     * @throws SQLException If preparing the SQL statement fails
     */
    protected void insertPlayerData(String insertPlayer, String[] playerDetails, Connection conn, int activityIndex) throws SQLException {
        PreparedStatement stmtPlayer = conn.prepareStatement(insertPlayer);
        stmtPlayer.setInt(1, Integer.parseInt(playerDetails[0]));
        stmtPlayer.setString(2, playerDetails[1]);
        //Most players have two name components
        if (activityIndex == 3) {
            stmtPlayer.setString(3, playerDetails[2].trim());
            stmtPlayer.setString(4, ScraperUtilsInterface.super.buildYear(playerDetails[4]));
            stmtPlayer.setString(5, ScraperUtilsInterface.super.buildYear(playerDetails[5]));
            stmtPlayer.setInt(6, Integer.parseInt(playerDetails[3]));
            //Some players have a single name
        } else if (activityIndex == 2) {
            stmtPlayer.setString(3, "");
            stmtPlayer.setString(4, ScraperUtilsInterface.super.buildYear(playerDetails[3]));
            stmtPlayer.setString(5, ScraperUtilsInterface.super.buildYear(playerDetails[4]));
            stmtPlayer.setInt(6, Integer.parseInt(playerDetails[2]));
            //Some players have three names
        } else if (activityIndex == 4) {
            stmtPlayer.setString(3, playerDetails[2].trim());
            stmtPlayer.setString(4, ScraperUtilsInterface.super.buildYear(playerDetails[5]));
            stmtPlayer.setString(5, ScraperUtilsInterface.super.buildYear(playerDetails[6]));
            stmtPlayer.setInt(6, Integer.parseInt(playerDetails[4]));
        }
        try {
            stmtPlayer.execute();
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    /**
     * Maps each player's current active status to their player ID
     *
     * @return hashmap with a key of player ID and a value of their currently active status
     * @throws SQLException If SQL query fails
     */
    private HashMap<Integer, Integer> getEachPlayerCurrentlyActiveMap(Connection connPlayers1) throws SQLException {
        ResultSet allPlayersWithCurrentlyActiveRS = connPlayers1.prepareStatement("SELECT id,currentlyactive from player_all_data ").executeQuery();
        HashMap<Integer, Integer> mapPlayerToCurrentlyActive = new HashMap<>();
        while (allPlayersWithCurrentlyActiveRS.next()) {
            mapPlayerToCurrentlyActive.put(allPlayersWithCurrentlyActiveRS.getInt("id"), allPlayersWithCurrentlyActiveRS.getInt("currentlyactive"));
        }
        return mapPlayerToCurrentlyActive;
    }

    /**
     * Maps each player's most recent active year to their player ID
     *
     * @return hashmap with a key of player ID and a value of their most recent active year
     * @throws SQLException If SQL query fails
     */
    private HashMap<Integer, Integer> getEachPlayerRecentYearMap(Connection connPlayers1) throws SQLException {
        ResultSet allPlayersRecentYearRS = connPlayers1.prepareStatement("SELECT id,mostrecentactiveyear from player_all_data ").executeQuery();
        HashMap<Integer, Integer> mapPlayerToRecentYear = new HashMap<>();
        while (allPlayersRecentYearRS.next()) {
            mapPlayerToRecentYear.put(allPlayersRecentYearRS.getInt("id"), Integer.parseInt(allPlayersRecentYearRS.getString("mostrecentactiveyear").substring(0, 4)));
        }
        return mapPlayerToRecentYear;
    }

    /**
     * Updates a player's currently active status to active or inactive
     *
     * @param isActive     the new activity status
     * @param id           the player's ID
     * @param connPlayers1 the first database connection
     * @param connPlayers2 the second database connection
     */
    private void updateCurrentlyActive(int isActive, String id, Connection connPlayers1, Connection connPlayers2) {
        String[] tableNames = new String[]{"player_all_data", "player_relevant_data"};
        for (String eachTableName : tableNames) {
            String sqlUpdate = "UPDATE " + eachTableName + " SET currentlyactive = ? WHERE id = ?";
            try {
                PreparedStatement stmt = connPlayers1.prepareStatement(sqlUpdate);
                stmt.setInt(1, isActive);
                stmt.setString(2, id);
                stmt.execute();
                if (connPlayers1 != connPlayers2) {
                    stmt = connPlayers2.prepareStatement(sqlUpdate);
                    stmt.setInt(1, isActive);
                    stmt.setString(2, id);
                    stmt.execute();
                }
            } catch (SQLException ex) {
                LOGGER.error(ex.getMessage());
            }
        }
    }

    /**
     * Updates a player's most recent active year
     *
     * @param playerDetails the player's basic information
     * @param index         the index of the player's most recent active year
     * @param connPlayers1  the first database connection
     * @param connPlayers2  the second database connection
     */
    protected void updateMostRecentActiveYear(String[] playerDetails, int index, Connection connPlayers1, Connection connPlayers2) {
        String[] tableNames = new String[]{"player_all_data", "player_relevant_data"};
        for (String eachTableName : tableNames) {
            if (eachTableName.equals("player_all_data") || (eachTableName.equals("player_relevant_data") && Integer.parseInt(playerDetails[index]) >= 1996)) {
                String sqlUpdate = "UPDATE " + eachTableName + " SET mostrecentactiveyear = ? WHERE id = ?";
                try {
                    PreparedStatement stmt = connPlayers1.prepareStatement(sqlUpdate);
                    stmt.setString(1, ScraperUtilsInterface.super.buildYear(playerDetails[index]));
                    stmt.setInt(2, Integer.parseInt(playerDetails[0]));
                    stmt.execute();
                    if (connPlayers1 != connPlayers2) {
                        stmt = connPlayers2.prepareStatement(sqlUpdate);
                        stmt.setString(1, ScraperUtilsInterface.super.buildYear(playerDetails[index]));
                        stmt.setInt(2, Integer.parseInt(playerDetails[0]));
                        stmt.execute();
                    }
                } catch (SQLException ex) {
                    LOGGER.error(ex.getMessage());
                }
            }
        }
    }


    /**
     * Inserts team information into database
     *
     * @param conn        connection to database
     * @param teamDetails array of team information
     */
    private void addTeamToDatabase(Connection conn, String[] teamDetails) {
        try {
            String insertTeam = "INSERT INTO team_data(id, abbr, casualname, firstname, secondname) VALUES (?,?,?,?,?)";
            PreparedStatement stmtTeam = conn.prepareStatement(insertTeam);
            stmtTeam.setInt(1, Integer.parseInt(teamDetails[0]));
            stmtTeam.setString(2, teamDetails[1]);
            stmtTeam.setString(3, teamDetails[2]);
            stmtTeam.setString(4, teamDetails[3]);
            stmtTeam.setString(5, teamDetails[4]);
            stmtTeam.execute();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Parses search response for all team data and inserts it into database
     *
     * @param teams        array of each team's data as a String, keeping the format from the source
     * @param connPlayers1 connection to first player database
     * @param connPlayers2 connection to second player database
     * @throws SQLException If saving team data fails
     */
    protected void processTeamData(String[] teams, Connection connPlayers1, Connection connPlayers2) throws SQLException {
        HashSet<Integer> allTeamIDs = getAllIds("SELECT id FROM team_data", connPlayers1, connPlayers2);
        String[] teamDetails;
        for (int teamIndex = 0; teamIndex < teams.length; teamIndex++) {
            /*
                Remove leading comma except for first entry
                Replace non-alphanumeric characters
                Split on comma
             */
            teamDetails = (teamIndex == 0 ? teams[teamIndex] : teams[teamIndex].replaceFirst(",", ""))
                    .replaceAll("[\\[\\]\\:\\}\\;\"]", "")
                    .split("\\,");
            //Check database for existing teams
            if (!teamDetails[0].equals("") && !allTeamIDs.contains(Integer.parseInt(teamDetails[0]))) {
                LOGGER.info("Team does not exist in DB: " + teamDetails[3] + " " + teamDetails[4]);
                addTeamToDatabase(connPlayers1, teamDetails);
                if (connPlayers1 != connPlayers2) {
                    addTeamToDatabase(connPlayers2, teamDetails);
                }
            } else {
                //Some entries are empty
                if (teamDetails.length > 3) {
                    LOGGER.info("Team already exists in DB: " + teamDetails[3] + " " + teamDetails[4]);
                }
            }
        }
    }

    /**
     * Parses search response for all player data and inserts it into database
     *
     * @param players                  array of each player's data as a String, keeping the format from the source
     * @param updatedPlayerActivities  hashmap of players with updated activity statuses for logging purposes
     * @param updatedLatestActiveYears hashmap of players with updated latest active years for logging purposes
     * @param connPlayers1             connection to first player database
     * @param connPlayers2             connection to second player database
     * @throws SQLException If setting or updating player data fails
     */
    protected void processPlayerData(String[] players, HashMap<String, Integer> updatedPlayerActivities, HashMap<String, Integer> updatedLatestActiveYears, Connection connPlayers1, Connection connPlayers2) throws SQLException {
        String insertPlayerAll = "INSERT INTO player_all_data (id, lastname ,firstname,firstactiveyear,mostrecentactiveyear,currentlyactive) VALUES (?,?,?,?,?,?) ";
        String insertPlayerRelevant = "INSERT INTO player_relevant_data (id, lastname ,firstname,firstactiveyear,mostrecentactiveyear,currentlyactive) VALUES (?,?,?,?,?,?) ";
        //Create array of player detail arrays
        String[] playerDetails;
        int[] importantIndexes;
        //Get ids already in database
        HashSet<Integer> allPlayerIDs = getAllIds("SELECT id FROM player_all_data", connPlayers1, connPlayers2);
        HashMap<Integer, Integer> mapPlayersToCurrentlyActive = getEachPlayerCurrentlyActiveMap(connPlayers1);
        HashMap<Integer, Integer> mapPlayersToRecentYear = getEachPlayerRecentYearMap(connPlayers1);
        //Iterate through array of player detail arrays
        for (int playerIndex = 0; playerIndex < players.length; playerIndex++) {
            /* For each player detail array:
                Remove leading comma except for first entry
                Replace non-alphanumeric characters
                Split on comma
             */
            playerDetails = (playerIndex == 0 ? players[playerIndex] : players[playerIndex].replaceFirst(",", ""))
                    .replaceAll("[\\[\\]\\:\\}\\;\"]", "")
                    .split("\\,");
            //Get activity, first year, latest year indexes
            importantIndexes = findImportantIndexes(playerDetails);
            int activityIndex = importantIndexes[0];
            int firstYearIndex = importantIndexes[1];
            int latestYearIndex = importantIndexes[2];
            if (activityIndex == 0 || firstYearIndex == 0 || latestYearIndex == 0) {
                continue;
            }
            //Check if player exists in all players database
            if (!playerDetails[0].equals("") && !allPlayerIDs.contains(Integer.parseInt(playerDetails[0]))) {
                //If player doesn't already exist, add to database
                insertPlayerData(insertPlayerAll, playerDetails, connPlayers1, activityIndex);
                if (connPlayers1 != connPlayers2) {
                    insertPlayerData(insertPlayerAll, playerDetails, connPlayers2, activityIndex);
                }
                LOGGER.info("Added player: " + playerDetails[2] + " " + playerDetails[1]);
                //Relevant players only after 1996-97
                if (Integer.parseInt(playerDetails[latestYearIndex]) >= 1996) {
                    insertPlayerData(insertPlayerRelevant, playerDetails, connPlayers1, activityIndex);
                    if (connPlayers1 != connPlayers2) {
                        insertPlayerData(insertPlayerRelevant, playerDetails, connPlayers2, activityIndex);
                    }
                }
                //Record for logging
                updatedPlayerActivities.put(playerDetails[2] + " " + playerDetails[1], Integer.parseInt(playerDetails[activityIndex]));
                updatedLatestActiveYears.put(playerDetails[2] + " " + playerDetails[1], Integer.parseInt(playerDetails[latestYearIndex]));
            } else {
                //If the data is consistent with the data in the database
                if (Integer.parseInt(playerDetails[activityIndex]) == mapPlayersToCurrentlyActive.get(Integer.parseInt(playerDetails[0]))
                        && Integer.parseInt(playerDetails[latestYearIndex]) == mapPlayersToRecentYear.get(Integer.parseInt(playerDetails[0]))) {
                    continue;
                }
                //If scraped activity status is different from the activity status in database
                if (Integer.parseInt(playerDetails[activityIndex]) != mapPlayersToCurrentlyActive.get(Integer.parseInt(playerDetails[0]))) {
                    //Set the player as (in)active in both tables
                    int activity = Integer.parseInt(playerDetails[activityIndex]);
                    updateCurrentlyActive(activity, playerDetails[0], connPlayers1, connPlayers2);
                    LOGGER.info("Updated " + playerDetails[1] + "," + playerDetails[2] + " CurrentlyActive with " + activity);
                    //Log activity changed
                    updatedPlayerActivities.put(playerDetails[2] + " " + playerDetails[1], activity);
                }
                //If the most recent active year is inconsistent with the database
                if (Integer.parseInt(playerDetails[latestYearIndex]) != mapPlayersToRecentYear.get(Integer.parseInt(playerDetails[0]))) {
                    updateMostRecentActiveYear(playerDetails, latestYearIndex, connPlayers1, connPlayers2);
                    LOGGER.info("Updated " + playerDetails[1] + "," + playerDetails[2] + " MostRecentActiveYear from "
                            + mapPlayersToRecentYear.get(Integer.parseInt(playerDetails[0])) + " to " + playerDetails[latestYearIndex]);
                    //Log changes
                    updatedLatestActiveYears.put(playerDetails[2] + " " + playerDetails[1], Integer.parseInt(playerDetails[latestYearIndex]));
                }
            }
        }
    }

    /**
     * Finds the indexes of current player activity status, first active year, and last active year
     *
     * @param playerDetails Array of player information
     * @return Array of indexes
     */
    protected int[] findImportantIndexes(String[] playerDetails) {
        //Activity, first year, latest year
        int[] indexes = new int[]{0, 0, 0};
        //Find array index of the player activity status
        for (int j = 0; j < playerDetails.length; j++) {
            if ((playerDetails[j].equals("0") || playerDetails[j].equals("1")) && indexes[0] == 0) {
                indexes[0] = j;
            }
            if (playerDetails[j].matches("-?\\d+") && playerDetails[j].length() == 4 && indexes[1] == 0) {
                indexes[1] = j;
                indexes[2] = j + 1;
            }
        }
        return indexes;
    }

    /**
     * Logs to console the changes applied to player database
     *
     * @param updatedPlayerActivities  HashMap with K,V pairs of PlayerName, Activity Status
     * @param updatedLatestActiveYears HashMap with K,V pairs of PlayerName, Latest Active Year
     */
    private void logScrapingResults(HashMap<String, Integer> updatedPlayerActivities, HashMap<String, Integer> updatedLatestActiveYears) {
        //Log results if there are changes made
        if (!updatedPlayerActivities.isEmpty()) {
            StringBuilder updatedPlayersOutput = new StringBuilder("Updated Player Activity:\n");
            updatedPlayerActivities.keySet()
                    .forEach(eachPlayer -> updatedPlayersOutput.append(eachPlayer).append(" (").append(updatedPlayerActivities.get(eachPlayer)).append(")\n"));
            LOGGER.info(updatedPlayersOutput.toString());
        } else {
            LOGGER.info("No player activities updated");
        }
        if (!updatedLatestActiveYears.isEmpty()) {
            StringBuilder updatedPlayersOutput = new StringBuilder("Updated Player Latest Active Year:\n");
            updatedLatestActiveYears.keySet().forEach(eachPlayer -> updatedPlayersOutput.append(eachPlayer).append(" (").append(updatedLatestActiveYears.get(eachPlayer)).append(")\n"));
            LOGGER.info(updatedPlayersOutput.toString());
        } else {
            LOGGER.info("No player latest years updated");
        }
    }
}
