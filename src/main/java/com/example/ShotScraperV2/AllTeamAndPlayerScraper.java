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
 * Scrapes and inserts NBA team and player data into the desired database
 */
@Component
public class AllTeamAndPlayerScraper implements ScraperUtilsInterface {
    private final Logger LOGGER = LoggerFactory.getLogger(AllTeamAndPlayerScraper.class);
    /**
     * Connection to first player database
     */
    private Connection connPlayers1 = null;
    /**
     * Connection to first second database
     */
    private Connection connPlayers2 = null;

    /**
     * Establish connections to databases
     *
     * @param schema1   Name of first database schema
     * @param location1 Location of first database
     * @param schema2   Name of second database schema
     * @param location2 Location of second database schema
     * @throws SQLException If connections to databases are denied
     */
    @Autowired
    public AllTeamAndPlayerScraper(@Value("${playerschema1}") String schema1,
                                   @Value("${playerlocation1}") String location1,
                                   @Value("${playerschema2}") String schema2,
                                   @Value("${playerlocation2}") String location2) throws SQLException {
        connPlayers1 = ScraperUtilsInterface.super.setNewConnection(schema1, location1);
        connPlayers2 = ScraperUtilsInterface.super.setNewConnection(schema2, location2);
    }

    /**
     * Creates player_all_data, player_relevant_data, team_data, and misc tables if they are not present in the connected database
     * <p>
     * The misc table is initialized with specific fields
     *
     * @param conn The connection to the desired database
     * @throws SQLException If a connection to the desired database cannot be established
     */
    private void createGeneralTablesIfNecessary(Connection conn) throws SQLException {
        HashSet<String> allPlayerTables = new HashSet();
        ResultSet allTablesRS = conn.prepareStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = 'nbaplayerinfov2'").executeQuery();
        while (allTablesRS.next()) {
            allPlayerTables.add(allTablesRS.getString("table_name"));
        }
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
            try {
                String sqlInsert = "INSERT INTO misc (type,value) VALUES ('version',null),('announcement',null),('lastshotsadded',null),('current year', null)";
                conn.prepareStatement(sqlInsert).execute();
            } catch (Exception ex) {
                LOGGER.error(ex.getMessage());
            }
        }
    }

    /**
     * Create a player table for players and their basic information and activity data
     *
     * @param tableName The name of the table
     * @param conn      Connection to database
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
     * Get all entries for column 'id' for given sql query
     *
     * @param sql The query to be executed
     * @return A HashSet of all ids
     * @throws SQLException If the query fails
     */
    private HashSet getAllIds(String sql) throws SQLException {
        ResultSet resultSet1 = connPlayers1.prepareStatement(sql).executeQuery();
        ResultSet resultSet2 = connPlayers2.prepareStatement(sql).executeQuery();
        HashSet<Integer> uniqueIds = new HashSet();
        while (resultSet1.next()) {
            uniqueIds.add(resultSet1.getInt("id"));
        }
        while (resultSet2.next()) {
            uniqueIds.add(resultSet1.getInt("id"));
        }
        resultSet1.close();
        resultSet2.close();
        return uniqueIds;
    }

    /**
     * Inserts basic player data and basic activity information into database
     *
     * @param insertPlayer  SQL statement, formatted for a prepared statement
     * @param playerDetails An array of player data scraped from site
     * @param conn          Connection to database
     * @param activityIndex Array index of data that indicates whether the player is active
     *                      <p>
     *                      Some players have more or fewer than two parts to their name
     *                      <p>
     *                      Active players have an additional parameter of their current team
     * @throws SQLException If preparing the SQL statement fails
     */
    private void insertPlayerData(String insertPlayer, String[] playerDetails, Connection conn, int activityIndex) throws SQLException {
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
            //Some players have a three names
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
     * Map each player ID to their current active status
     *
     * @return A Hashmap with a key of player ID and a value of their currently active status
     * @throws SQLException If SQL query fails
     */
    private HashMap getEachPlayerCurrentlyActiveMap() throws SQLException {
        ResultSet allPlayersWithCurrentlyActiveRS = connPlayers1.prepareStatement("SELECT id,currentlyactive from player_all_data ").executeQuery();
        HashMap<Integer, Integer> mapPlayerToCurrentlyActive = new HashMap<>();
        while (allPlayersWithCurrentlyActiveRS.next()) {
            mapPlayerToCurrentlyActive.put(allPlayersWithCurrentlyActiveRS.getInt("id"), allPlayersWithCurrentlyActiveRS.getInt("currentlyactive"));
        }
        return mapPlayerToCurrentlyActive;
    }

    /**
     * Map each player ID to their most recent active year
     *
     * @return A Hashmap with a key of player ID and a value of their most recent active year
     * @throws SQLException If SQL query fails
     */
    private HashMap getEachPlayerRecentYearMap() throws SQLException {
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
     * @param tableName Table name to update
     * @param isActive  New activity status
     * @param id        Player ID
     * @param conn      Database connection
     * @throws SQLException If updating fails
     */
    private void updateCurrentlyActive(String tableName, int isActive, String id, Connection conn) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("UPDATE " + tableName + " SET currentlyactive = ? WHERE id = ?");
        stmt.setInt(1, isActive);
        stmt.setString(2, id);
        stmt.execute();
    }

    /**
     * Updates a player's most recent active year
     *
     * @param playerDetails A player's basic information
     * @param index         The index of the player's most recent active year
     */
    protected void updateMostRecentActiveYear(String[] playerDetails, int index) {
        String sqlUpdate = "UPDATE player_all_data SET mostrecentactiveyear = \"" + ScraperUtilsInterface.super.buildYear(playerDetails[index]) + "\" WHERE id = " + Integer.parseInt(playerDetails[0]);
        try {
            connPlayers2.prepareStatement(sqlUpdate).execute();
            connPlayers1.prepareStatement(sqlUpdate).execute();
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
        if (Integer.parseInt(playerDetails[index]) >= 1996) {
            sqlUpdate = "UPDATE player_relevant_data SET mostrecentactiveyear = \"" + ScraperUtilsInterface.super.buildYear(playerDetails[index]) + "\" WHERE id = " + Integer.parseInt(playerDetails[0]);
            try {
                connPlayers2.prepareStatement(sqlUpdate).execute();
                connPlayers1.prepareStatement(sqlUpdate).execute();
            } catch (SQLException ex) {
                LOGGER.error(ex.getMessage());
            }
        }
    }

    /**
     * Scrapes all basic team data and all basic player data
     *
     * @throws InterruptedException If fetching the site is interrupted
     * @throws IOException          If fetching the site is interrupted
     * @throws SQLException         If inserting the data into a database fails
     */
    public void getTeamAndPlayerData() throws InterruptedException, IOException, SQLException {
        createGeneralTablesIfNecessary(connPlayers1);
        createGeneralTablesIfNecessary(connPlayers2);
        String url = "https://www.nba.com/stats/js/data/ptsd/stats_ptsd.js";
        String response = ScraperUtilsInterface.super.fetchSpecificURL(url);
        HashSet<Integer> allTeamIDs = getAllIds("SELECT id FROM team_data");
        HashSet<Integer> allPlayerIDs = getAllIds("SELECT id FROM player_all_data");
        HashMap<Integer, Integer> mapPlayersToCurrentlyActive = getEachPlayerCurrentlyActiveMap();
        HashMap<Integer, Integer> mapPlayersToRecentYear = getEachPlayerRecentYearMap();
        //Record updated players, activity status, and active years for logging the results
        HashMap<String, Integer> updatedPlayerActivities = new HashMap();
        HashMap<String, Integer> updatedLatestActiveYears = new HashMap();
        String[] data = response.split("\"teams\"")[1].split("\"players\"");
        String[] teams = data[0].split("\\]\\]");
        String[] players = data[1].split("\\]");
        String[] teamDetails, playerDetails;
        //Parse the team data
        for (int teamIndex = 0; teamIndex < teams.length; teamIndex++) {
            teamDetails = (teamIndex == 0 ? teams[teamIndex] : teams[teamIndex].replaceFirst(",", "")).replaceAll("[\\[\\]\\:\\}\\;\"]", "").split("\\,");
            if (!teamDetails[0].equals("") && !allTeamIDs.contains(Integer.parseInt(teamDetails[0]))) {
                LOGGER.info("Team does not exist in DB: " + teamDetails[3] + " " + teamDetails[4]);
                addTeamToDatabase(connPlayers1, teamDetails);
                addTeamToDatabase(connPlayers2, teamDetails);
            } else {
                if (teamDetails.length > 3) {
                    LOGGER.info("Team already exists in DB: " + teamDetails[3] + " " + teamDetails[4]);
                }
            }
        }
        //Parse the player data
        String insertPlayerAll = "INSERT INTO player_all_data (id, lastname ,firstname,firstactiveyear,mostrecentactiveyear,currentlyactive) VALUES (?,?,?,?,?,?) ";
        for (int playerIndex = 0; playerIndex < players.length; playerIndex++) {
            playerDetails = (playerIndex == 0 ? players[playerIndex] : players[playerIndex].replaceFirst(",", "")).replaceAll("[\\[\\]\\:\\}\\;\"]", "").split("\\,");
            int activityIndex = 0;
            int firstYearIndex = 0;
            int latestYearIndex = 0;
            //Find array index of the player activity status
            for (int j = 0; j < playerDetails.length; j++) {
                if ((playerDetails[j].equals("0") || playerDetails[j].equals("1")) && activityIndex == 0) {
                    activityIndex = j;
                }
                if (playerDetails[j].matches("-?\\d+") && playerDetails[j].length() == 4 && firstYearIndex == 0) {
                    firstYearIndex = j;
                    latestYearIndex = j + 1;
                }
            }
            if (activityIndex == 0 || firstYearIndex == 0 || latestYearIndex == 0) {
                continue;
            }
            //Check if player already exists in the database
            if (!playerDetails[0].equals("") && !allPlayerIDs.contains(Integer.parseInt(playerDetails[0]))) {
                LOGGER.info("Player does not exist in DB: " + playerDetails[2] + " " + playerDetails[1]);
                insertPlayerData(insertPlayerAll, playerDetails, connPlayers1, activityIndex);
                insertPlayerData(insertPlayerAll, playerDetails, connPlayers2, activityIndex);
                if (Integer.parseInt(playerDetails[latestYearIndex]) >= 1996) {
                    String insertPlayer2 = "INSERT INTO player_relevant_data (id, lastname ,firstname,firstactiveyear,mostrecentactiveyear,currentlyactive) VALUES (?,?,?,?,?,?) ";
                    insertPlayerData(insertPlayer2, playerDetails, connPlayers1, activityIndex);
                    insertPlayerData(insertPlayer2, playerDetails, connPlayers2, activityIndex);
                }
                updatedPlayerActivities.put(playerDetails[2] + " " + playerDetails[1], Integer.parseInt(playerDetails[activityIndex]));
                updatedLatestActiveYears.put(playerDetails[2] + " " + playerDetails[1], Integer.parseInt(playerDetails[latestYearIndex]));
            } else {
                //If the data is consistent with the data in the database
                if (Integer.parseInt(playerDetails[activityIndex]) == mapPlayersToCurrentlyActive.get(Integer.parseInt(playerDetails[0]))
                        && Integer.parseInt(playerDetails[latestYearIndex]) == mapPlayersToRecentYear.get(Integer.parseInt(playerDetails[0]))) {
                    continue;
                }
                if (Integer.parseInt(playerDetails[activityIndex]) != mapPlayersToCurrentlyActive.get(Integer.parseInt(playerDetails[0]))) {
                    //Set the player as active in both tables
                    int activity = Integer.parseInt(playerDetails[activityIndex]);
                    updateCurrentlyActive("player_all_data", activity, playerDetails[0], connPlayers1);
                    updateCurrentlyActive("player_relevant_data", activity, playerDetails[0], connPlayers1);
                    updateCurrentlyActive("player_all_data", activity, playerDetails[0], connPlayers2);
                    updateCurrentlyActive("player_relevant_data", activity, playerDetails[0], connPlayers2);
                    LOGGER.info("Updated " + playerDetails[1] + "," + playerDetails[2] + " CurrentlyActive with " + activity);
                    updatedPlayerActivities.put(playerDetails[2] + " " + playerDetails[1], activity);
                }
                //If the most recent active year is inconsistent with the database
                if (Integer.parseInt(playerDetails[latestYearIndex]) != mapPlayersToRecentYear.get(Integer.parseInt(playerDetails[0]))) {
                    updateMostRecentActiveYear(playerDetails, latestYearIndex);
                    LOGGER.info("Updated " + playerDetails[1] + "," + playerDetails[2] + " MostRecentActiveYear from "
                            + mapPlayersToRecentYear.get(Integer.parseInt(playerDetails[0])) + " to " + playerDetails[latestYearIndex]);
                    updatedLatestActiveYears.put(playerDetails[2] + " " + playerDetails[1], Integer.parseInt(playerDetails[latestYearIndex]));
                }
            }
        }
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
        LOGGER.info("End of getTeamAndPlayerData()");
    }

    /**
     * Insert team information into database
     * @param conn Connection to database
     * @param teamDetails Array of team information
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
}
