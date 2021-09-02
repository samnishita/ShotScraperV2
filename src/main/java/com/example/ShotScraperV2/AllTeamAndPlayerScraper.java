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

@Component
public class AllTeamAndPlayerScraper {
    private final Logger LOGGER = LoggerFactory.getLogger(AllTeamAndPlayerScraper.class);
    private Connection connPlayers1 = null, connPlayers2 = null;

    private ScraperUtilityFunctions scraperUtilityFunctions = new ScraperUtilityFunctions();

    @Autowired
    public AllTeamAndPlayerScraper(@Value("${playerschema1}") String schema1,
                                   @Value("${playerlocation1}") String location1,
                                   @Value("${playerschema2}") String schema2,
                                   @Value("${playerlocation2}") String location2) throws SQLException {
        connPlayers1 = scraperUtilityFunctions.setNewConnection(schema1, location1);
        connPlayers2 = scraperUtilityFunctions.setNewConnection(schema2, location2);
        createGeneralTablesIfNecessary(connPlayers1);
        createGeneralTablesIfNecessary(connPlayers2);
        createMiscTable();
    }

    private void createGeneralTablesIfNecessary(Connection conn) throws SQLException {
        HashSet<String> allPlayerTables = new HashSet();
        ResultSet allTablesRS = conn.prepareStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = 'nbaplayerinfov2'").executeQuery();
        while (allTablesRS.next()) {
            allPlayerTables.add(allTablesRS.getString("table_name"));
        }
        if (!allPlayerTables.contains("player_all_data")) {
            createLargePlayerTable("player_all_data");
        }
        if (!allPlayerTables.contains("player_relevant_data")) {
            createLargePlayerTable("player_relevant_data");
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
    }

    private void createLargePlayerTable(String tableName) throws SQLException {
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
        this.connPlayers1.prepareStatement(sqlCT).execute();
        this.connPlayers2.prepareStatement(sqlCT).execute();
    }

    private void createMiscTable() throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS `misc` (\n"
                + "`type` varchar(20) NOT NULL,\n"
                + "`value` varchar(200) DEFAULT NULL,\n"
                + "UNIQUE KEY `type_UNIQUE` (`type`)\n"
                + ")\n"
                + "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";
        connPlayers1.prepareStatement(sql).execute();
        connPlayers2.prepareStatement(sql).execute();
        try {
            String sqlInsert = "INSERT INTO misc (type,value) VALUES ('version',null),('announcement',null),('lastshotsadded',null)";
            connPlayers1.prepareStatement(sqlInsert).execute();
            connPlayers2.prepareStatement(sqlInsert).execute();
        } catch (Exception ex) {

        }
    }

    private HashSet getAllIds(String sql) throws SQLException {
        ResultSet resultSet = connPlayers1.prepareStatement(sql).executeQuery();
        HashSet<Integer> uniqueIds = new HashSet();
        while (resultSet.next()) {
            uniqueIds.add(resultSet.getInt("id"));
        }
        return uniqueIds;
    }

    private void insertPlayerData(String insertPlayer, String[] playerDetails, Connection conn, int activityIndex) throws SQLException {
        PreparedStatement stmtPlayer = conn.prepareStatement(insertPlayer);
        stmtPlayer.setInt(1, Integer.parseInt(playerDetails[0]));
        stmtPlayer.setString(2, playerDetails[1]);
        if (activityIndex == 3) {
            stmtPlayer.setString(3, playerDetails[2].trim());
            stmtPlayer.setString(4, scraperUtilityFunctions.buildYear(playerDetails[4]));
            stmtPlayer.setString(5, scraperUtilityFunctions.buildYear(playerDetails[5]));
            stmtPlayer.setInt(6, Integer.parseInt(playerDetails[3]));
        } else if (activityIndex == 2) {
            stmtPlayer.setString(3, "");
            stmtPlayer.setString(4, scraperUtilityFunctions.buildYear(playerDetails[3]));
            stmtPlayer.setString(5, scraperUtilityFunctions.buildYear(playerDetails[4]));
            stmtPlayer.setInt(6, Integer.parseInt(playerDetails[2]));
        } else if (activityIndex == 4) {
            stmtPlayer.setString(3, playerDetails[2].trim());
            stmtPlayer.setString(4, scraperUtilityFunctions.buildYear(playerDetails[5]));
            stmtPlayer.setString(5, scraperUtilityFunctions.buildYear(playerDetails[6]));
            stmtPlayer.setInt(6, Integer.parseInt(playerDetails[4]));
        }
        try {
            stmtPlayer.execute();
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
    }


    private HashMap getEachPlayerCurrentlyActiveMap() throws SQLException {
        ResultSet allPlayersWithCurrentlyActiveRS = connPlayers1.prepareStatement("SELECT id,currentlyactive from player_all_data ").executeQuery();
        HashMap<Integer, Integer> mapPlayerToCurrentlyActive = new HashMap<>();
        while (allPlayersWithCurrentlyActiveRS.next()) {
            mapPlayerToCurrentlyActive.put(allPlayersWithCurrentlyActiveRS.getInt("id"), allPlayersWithCurrentlyActiveRS.getInt("currentlyactive"));
        }
        return mapPlayerToCurrentlyActive;
    }

    private HashMap getEachPlayerRecentYearMap() throws SQLException {
        ResultSet allPlayersRecentYearRS = connPlayers1.prepareStatement("SELECT id,mostrecentactiveyear from player_all_data ").executeQuery();
        HashMap<Integer, Integer> mapPlayerToRecentYear = new HashMap<>();
        while (allPlayersRecentYearRS.next()) {
            mapPlayerToRecentYear.put(allPlayersRecentYearRS.getInt("id"), Integer.parseInt(allPlayersRecentYearRS.getString("mostrecentactiveyear").substring(0, 4)));
        }
        return mapPlayerToRecentYear;
    }


    private void updateCurrentlyActive(String tableName, int isActive, String id, Connection conn) throws SQLException {
        String sqlUpdate = "UPDATE " + tableName + " SET currentlyactive = " + isActive + " WHERE id = " + id;
        conn.prepareStatement(sqlUpdate).execute();
    }

    protected void updateMostRecentActiveYear(String[] playerDetails, int index) {
        String sqlUpdate = "UPDATE player_all_data SET mostrecentactiveyear = \"" + scraperUtilityFunctions.buildYear(playerDetails[index]) + "\" WHERE id = " + Integer.parseInt(playerDetails[0]);
        try {
            connPlayers2.prepareStatement(sqlUpdate).execute();
            connPlayers1.prepareStatement(sqlUpdate).execute();
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
        if (Integer.parseInt(playerDetails[index]) >= 1996) {
            sqlUpdate = "UPDATE player_relevant_data SET mostrecentactiveyear = \"" + scraperUtilityFunctions.buildYear(playerDetails[index]) + "\" WHERE id = " + Integer.parseInt(playerDetails[0]);
            try {
                connPlayers2.prepareStatement(sqlUpdate).execute();
                connPlayers1.prepareStatement(sqlUpdate).execute();
            } catch (SQLException ex) {
                LOGGER.error(ex.getMessage());
            }
        }
    }

    public void getTeamAndPlayerData() throws InterruptedException, IOException, SQLException {
        String url = "https://www.nba.com/stats/js/data/ptsd/stats_ptsd.js";
        String response = scraperUtilityFunctions.fetchSpecificURL(url);
        HashSet<Integer> allTeamIDs = getAllIds("SELECT id FROM team_data");
        HashSet<Integer> allPlayerIDs = getAllIds("SELECT id FROM player_all_data");
        HashMap<Integer, Integer> mapPlayersToCurrentlyActive = getEachPlayerCurrentlyActiveMap();
        HashMap<Integer, Integer> mapPlayersToRecentYear = getEachPlayerRecentYearMap();
        HashMap<String, Integer> updatedPlayerActivities = new HashMap();
        HashMap<String, Integer> updatedLatestActiveYears = new HashMap();
        String[] data = response.split("\"teams\"")[1].split("\"players\"");
        String[] teams = data[0].split("\\]\\]");
        String[] players = data[1].split("\\]");
        String[] teamDetails, playerDetails;
        for (int teamIndex = 0; teamIndex < teams.length; teamIndex++) {
            teamDetails = (teamIndex == 0 ? teams[teamIndex] : teams[teamIndex].replaceFirst(",", "")).replaceAll("[\\[\\]\\:\\}\\;\"]", "").split("\\,");
            if (!teamDetails[0].equals("") && !allTeamIDs.contains(Integer.parseInt(teamDetails[0]))) {
                LOGGER.info("Team does not exist in DB: " + teamDetails[3] + " " + teamDetails[4]);
                executeStatement(connPlayers1, teamDetails);
                executeStatement(connPlayers2, teamDetails);
            } else {
                if (teamDetails.length > 3) {
                    LOGGER.info("Team already exists in DB: " + teamDetails[3] + " " + teamDetails[4]);
                }
            }
        }
        String insertPlayerAll = "INSERT INTO player_all_data (id, lastname ,firstname,firstactiveyear,mostrecentactiveyear,currentlyactive) VALUES (?,?,?,?,?,?) ";
        for (int playerIndex = 0; playerIndex < players.length; playerIndex++) {
            playerDetails = (playerIndex == 0 ? players[playerIndex] : players[playerIndex].replaceFirst(",", "")).replaceAll("[\\[\\]\\:\\}\\;\"]", "").split("\\,");
            int activityIndex = 0;
            int firstYearIndex = 0;
            int latestYearIndex = 0;
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
                if (Integer.parseInt(playerDetails[latestYearIndex]) != mapPlayersToRecentYear.get(Integer.parseInt(playerDetails[0]))) {
                    updateMostRecentActiveYear(playerDetails, latestYearIndex);
                    LOGGER.info("Updated " + playerDetails[1] + "," + playerDetails[2] + " MostRecentActiveYear from "
                            + mapPlayersToRecentYear.get(Integer.parseInt(playerDetails[0])) + " to " + playerDetails[latestYearIndex]);
                    updatedLatestActiveYears.put(playerDetails[2] + " " + playerDetails[1], Integer.parseInt(playerDetails[latestYearIndex]));
                }
            }
        }
        if (!updatedPlayerActivities.isEmpty()) {
            StringBuilder updatedPlayersOutput = new StringBuilder("Updated Player Activity:\n");
            updatedPlayerActivities.keySet().forEach(eachPlayer -> updatedPlayersOutput.append(eachPlayer).append(" (").append(updatedPlayerActivities.get(eachPlayer)).append(")\n"));
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

    private void executeStatement(Connection conn, String[] teamDetails) {
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
