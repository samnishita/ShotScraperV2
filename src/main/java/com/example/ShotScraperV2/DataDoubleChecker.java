package com.example.ShotScraperV2;

import com.example.ShotScraperV2.nbaobjects.Player;
import com.example.ShotScraperV2.nbaobjects.Shot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Tool for verifying data is consistent between remote and local databases
 */
@Component
public class DataDoubleChecker implements ScraperUtilsInterface {
    private final Logger LOGGER = LoggerFactory.getLogger(DataDoubleChecker.class);
    /**
     * Tables from original scraping algorithm that are verified as incorrect
     */
    private ArrayList<String> knownWrongShotTables = new ArrayList<>(Arrays.asList("Jenkins_John_203098_2014_15_Playoffs", "Ibaka_Serge_201586_2013_14_Playoffs", "Leaf_TJ_1628388_2019_20_Preseason",
            "Grant_Jerami_203924_2017_18_Playoffs", "Davis_Anthony_203076_2015_16_Preseason", "Leaf_TJ_1628388_2018_19_Preseason", "Connaughton_Pat_1626192_2019_20_RegularSeason",
            "Leonard_Kawhi_202695_2020_21_RegularSeason", "Blair_DeJuan_201971_2010_11_Preseason", "Johnson_BJ_1629168_2019_20_Preseason", "Harkless_Maurice_203090_2013_14_RegularSeason",
            "Karasev_Sergey_203508_2014_15_RegularSeason", "Johnson_Nick_203910_2014_15_Preseason", "Leaf_TJ_1628388_2017_18_RegularSeason", "Shamet_Landry_1629013_2020_21_Playoffs",
            "Leaf_TJ_1628388_2018_19_RegularSeason", "Jefferson_Richard_2210_2005_06_Playoffs", "Leaf_TJ_1628388_2017_18_Preseason", "Leaf_TJ_1628388_2018_19_Playoffs", "Leaf_TJ_1628388_2019_20_RegularSeason",
            "Ajinca_Alexis_201582_2015_16_Preseason", "Leaf_TJ_1628388_2020_21_RegularSeason", "Beverley_Patrick_201976_2015_16_RegularSeason", "Leaf_TJ_1628388_2020_21_Playoffs",
            "Johnson_BJ_1629168_2018_19_RegularSeason", "Johnson_BJ_1629168_2019_20_Playoffs", "Montross_Eric_376_1996_97_RegularSeason", "Johnson_BJ_1629168_2019_20_RegularSeason",
            "Alabi_Solomon_202374_2010_11_Preseason", "Grant_Jerami_203924_2016_17_Playoffs", "Black_Tarik_204028_2014_15_Preseason", "Ajinca_Alexis_201582_2014_15_Playoffs",
            "LeVert_Caris_1627747_2018_19_Preseason", "all_time_zoned_averages", "all_shot_types"));
    private int tableCounter = 0;

    /**
     * Compares all player tables by total table count and each table's data
     *
     * @param dropMismatchedTables should drop mismatched tables
     */
    public void comparePlayerTables(boolean dropMismatchedTables, String schemaPlayers1Alias, String schemaPlayers2Alias) throws SQLException {
        Connection connPlayers1 = ScraperUtilsInterface.super.setNewConnection(schemaPlayers1Alias);
        Connection connPlayers2 = ScraperUtilsInterface.super.setNewConnection(schemaPlayers2Alias);
        //Compare total table counts
        HashSet<String> playerTableNamesHash1 = getTableNames(connPlayers1, schemaPlayers1Alias);
        HashSet<String> playerTableNamesHash2 = getTableNames(connPlayers2, schemaPlayers2Alias);
        LOGGER.info("DB1 Table Count: " + playerTableNamesHash1.size());
        LOGGER.info("DB2 Table Count: " + playerTableNamesHash2.size());
        //Find tables in one database that aren't in the other
        getEntitiesInDB1NotInDB2(playerTableNamesHash1, playerTableNamesHash2, schemaPlayers1Alias, schemaPlayers2Alias, "Tables in " + schemaPlayers1Alias + " not in " + schemaPlayers2Alias + ":\n");
        getEntitiesInDB1NotInDB2(playerTableNamesHash2, playerTableNamesHash1, schemaPlayers2Alias, schemaPlayers1Alias, "Tables in " + schemaPlayers2Alias + " not in " + schemaPlayers1Alias + ":\n");
        //Get all players with complete data
        HashSet<Player> allPlayers1 = getAllPlayers(connPlayers1);
        HashSet<Player> allPlayers2 = getAllPlayers(connPlayers2);
        //Compare each player's complete data
        LOGGER.info("Players in " + schemaPlayers1Alias + " with different data than in " + schemaPlayers2Alias + ":");
        allPlayers1.stream().filter(eachPlayer -> !allPlayers2.contains(eachPlayer))
                .forEach(eachPlayer -> LOGGER.info(eachPlayer.getPlayerId() + " " + eachPlayer.getFirstName() + " " + eachPlayer.getLastName()));
        LOGGER.info("Players in " + schemaPlayers2Alias + " with different data than in " + schemaPlayers1Alias + ":");
        allPlayers2.stream().filter(eachPlayer -> !allPlayers1.contains(eachPlayer))
                .forEach(eachPlayer -> LOGGER.info(eachPlayer.getPlayerId() + " " + eachPlayer.getFirstName() + " " + eachPlayer.getLastName()));
        connPlayers1.close();
        connPlayers2.close();
    }

    /**
     * Compares all shot tables by total table count and each table's data
     *
     * @param dropMismatchedTables should drop mismatched tables
     */
    public void compareShotTables(boolean dropMismatchedTables, boolean checkFullShots, String schemaPlayersAlias, String schemaShots1Alias, String schemaShots2Alias) throws SQLException {
        Connection connPlayers = ScraperUtilsInterface.super.setNewConnection(schemaPlayersAlias);
        Connection connShots1 = ScraperUtilsInterface.super.setNewConnection(schemaShots1Alias);
        Connection connShots2 = ScraperUtilsInterface.super.setNewConnection(schemaShots2Alias);
        //Compare total table counts
        HashSet<String> shotTableNamesHash1 = getTableNames(connShots1, schemaShots1Alias);
        HashSet<String> shotTableNamesHash2 = getTableNames(connShots2, schemaShots2Alias);
        LOGGER.info("DB1 Table Count: " + shotTableNamesHash1.size());
        LOGGER.info("DB2 Table Count: " + shotTableNamesHash2.size());
        //Find tables in one database that aren't in the other
        getEntitiesInDB1NotInDB2(shotTableNamesHash1, shotTableNamesHash2, schemaShots1Alias, schemaShots2Alias, "Tables in " + schemaShots1Alias + " not in " + schemaShots2Alias + ":\n");
        getEntitiesInDB1NotInDB2(shotTableNamesHash2, shotTableNamesHash1, schemaShots2Alias, schemaShots1Alias, "Tables in " + schemaShots2Alias + " not in " + schemaShots1Alias + ":\n");
        //Large table split into multiple smaller queries
        //For tables existing in both databases, compare each row (not including all_shots and known wrong tables)
        shotTableNamesHash1.stream()
                .filter(eachTableName -> shotTableNamesHash2.contains(eachTableName))
                .filter(eachTableName -> !eachTableName.equals("all_shots"))
                .filter(eachTableName -> !knownWrongShotTables.contains(eachTableName))
                .forEach(eachTableName -> compareShotTableData(eachTableName, dropMismatchedTables, checkFullShots, schemaShots1Alias, schemaShots2Alias, connShots1, connShots2));
        compareAll_ShotsTable(connPlayers, connShots1, connShots2, schemaShots1Alias, schemaShots2Alias, checkFullShots);
        connPlayers.close();
        connShots1.close();
        connShots2.close();
    }

    /**
     * Retrieves all table names from the database
     *
     * @param conn        connection to the database
     * @param schemaAlias alias of schema to be used
     * @return set of all tables
     */
    private HashSet<String> getTableNames(Connection conn, String schemaAlias) {
        HashSet<String> playerTableNamesHash = new HashSet<>();
        try {
            ResultSet playerTables = conn.getMetaData().getTables(ScraperUtilsInterface.super.getSchemaName(schemaAlias), null, "%", null);
            while (playerTables.next()) {
                playerTableNamesHash.add(playerTables.getString(3));
            }
            playerTables.close();
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
        return playerTableNamesHash;
    }

    /**
     * Compares two sets and returns all values present in one set that are not present in the other set
     *
     * @param entity1            set of interest
     * @param entity2            set to be compared against
     * @param schema1            first database schema
     * @param schema2            second database schema
     * @param stringBuilderStart start of logging message for results
     * @return items in entity1 not in entity2
     */
    private HashSet<String> getEntitiesInDB1NotInDB2(HashSet<String> entity1, HashSet<String> entity2, String schema1, String schema2, String stringBuilderStart) {
        HashSet<String> entitiesInDB1NotInDB2 = new HashSet<>();
        //For one set, check if entity (table name or row) is present in the other set
        //If it is not present, save to second set
        for (String eachEntity : entity1) {
            if (!entity2.contains(eachEntity)) {
                entitiesInDB1NotInDB2.add(eachEntity);
            }
        }
        //If all items in first set are present in second set
        if (entitiesInDB1NotInDB2.size() > 0) {
            StringBuilder entitiesInDB1NotInDB2Builder = new StringBuilder(stringBuilderStart);
            //Log all items from first set not present in second set
            if (!stringBuilderStart.contains("all_shots")) {
                entitiesInDB1NotInDB2.forEach(eachEntity -> entitiesInDB1NotInDB2Builder.append(eachEntity + "\n"));
            }
            entitiesInDB1NotInDB2Builder.append("TOTAL: ").append(entitiesInDB1NotInDB2.size());
//            if (entitiesInDB1NotInDB2.size() > 30) {
//                LOGGER.info(stringBuilderStart + "More than 30 entries");
//            } else {
//                LOGGER.info(entitiesInDB1NotInDB2Builder.toString());
//            }
            LOGGER.info(entitiesInDB1NotInDB2Builder.toString());

        }
        return entitiesInDB1NotInDB2;
    }

    /**
     * Compares all rows in a given shot table
     *
     * @param tableName  shot table name
     * @param dropTables should drop tables if they are mismatched
     */
    private void compareShotTableData(String tableName, boolean dropTables, boolean checkFullShots, String schemaShots1Alias, String schemaShots2Alias, Connection connShots1, Connection connShots2) {
        HashSet<String> db1Rows, db2Rows;
        if (checkFullShots) {
            //Analyze all columns
            db1Rows = createStringFromRow(connShots1, tableName);
            db2Rows = createStringFromRow(connShots2, tableName);
        } else {
            //Analyze only shot IDs
            db1Rows = findShotIds(connShots1, tableName);
            db2Rows = findShotIds(connShots2, tableName);
        }
        HashSet<String> rowsInDB1NotInDB2 = getEntitiesInDB1NotInDB2(db1Rows, db2Rows, schemaShots1Alias, schemaShots2Alias, "Rows found in " + schemaShots1Alias + "." + tableName + " not found in " + schemaShots2Alias + "." + tableName + ": \n");
        HashSet<String> rowsInDB2NotInDB1 = getEntitiesInDB1NotInDB2(db2Rows, db1Rows, schemaShots2Alias, schemaShots1Alias, "Rows found in " + schemaShots2Alias + "." + tableName + " not found in " + schemaShots1Alias + "." + tableName + ": \n");
        //Find which rows are different between the two sets
        if ((rowsInDB1NotInDB2.size() != 0 || rowsInDB2NotInDB1.size() != 0) && !knownWrongShotTables.contains(tableName)) {
            LOGGER.info("MISMATCHED TABLE: " + tableName);
            if (dropTables) {
                dropMismatchedTables(tableName, connShots1, schemaShots1Alias);
            }
        }
        tableCounter++;
        if (tableCounter % 1000 == 0) {
            LOGGER.info(tableCounter + "");
        }
    }

    /**
     * Concatenates each row retrieved from database
     *
     * @param conn      connection to database
     * @param tableName table name to query
     * @return set of all rows concatenated
     */
    private HashSet<String> createStringFromRow(Connection conn, String tableName) {
        HashSet<String> rowStrings = new HashSet<>();
        try {
            ResultSet dbResultSet = conn.prepareStatement("SELECT * FROM " + tableName).executeQuery();
            StringBuilder eachRowStringBuilder;
            while (dbResultSet.next()) {
                eachRowStringBuilder = new StringBuilder();
                //Concatenate with underscores between parameters
                for (int i = 1; i <= dbResultSet.getMetaData().getColumnCount(); i++) {
                    eachRowStringBuilder.append(dbResultSet.getString(i));
                    if (i != dbResultSet.getMetaData().getColumnCount()) {
                        eachRowStringBuilder.append("_");
                    }
                }
                rowStrings.add(eachRowStringBuilder.toString());
            }
            dbResultSet.close();
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
        return rowStrings;
    }

    /**
     * Drops table from database
     *
     * @param tableName   table to drop
     * @param conn        connection to database
     * @param schemaAlias database schema alias
     */
    private void dropMismatchedTables(String tableName, Connection conn, String schemaAlias) {
        try {
            conn.prepareStatement("DROP TABLE `" + ScraperUtilsInterface.super.getSchemaName(schemaAlias) + "`.`" + tableName + "`").execute();
            LOGGER.info("Dropping " + tableName + " from " + schemaAlias);
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    /**
     * Compares large table by doing multiple smaller comparisons
     */
    private void compareAll_ShotsTable(Connection connPlayers1, Connection connShots1, Connection connShots2, String schemaShots1Alias, String schemaShots2Alias, boolean checkFullShots) throws SQLException {
        if (schemaShots2Alias.equals("shottrusted")) {
            schemaShots2Alias = "shottrustedindexed";
            connShots2 = ScraperUtilsInterface.super.setNewConnection(schemaShots2Alias);
        }
        try {
            if (!checkFullShots) {
                HashSet<String> allShotsAsUniqueIds1 = findShotIds(connShots1, "all_shots");
                HashSet<String> allShotsAsUniqueIds2 = findShotIds(connShots2, "all_shots");
                getEntitiesInDB1NotInDB2(allShotsAsUniqueIds1, allShotsAsUniqueIds2, schemaShots1Alias, schemaShots2Alias, "Rows found in " + schemaShots1Alias + ".all_shots not found in " + schemaShots2Alias + ".all_shots: \n");
                getEntitiesInDB1NotInDB2(allShotsAsUniqueIds2, allShotsAsUniqueIds1, schemaShots2Alias, schemaShots1Alias, "Rows found in " + schemaShots2Alias + ".all_shots not found in " + schemaShots1Alias + ".all_shots: \n");
            } else {
                //Gather all player IDs
                HashSet<Integer> allPlayerIds = new HashSet<>();
                ResultSet playerIdResultSet = connPlayers1.prepareStatement("SELECT id FROM player_relevant_data").executeQuery();
                while (playerIdResultSet.next()) {
                    allPlayerIds.add(playerIdResultSet.getInt("id"));
                }
                playerIdResultSet.close();
                int counter = 0;
                for (Integer eachID : allPlayerIds) {
                    //Only compare shots from a single player at a time
                    HashSet<Shot> allShots1 = findShotsInAll_Shots(connShots1, eachID, checkFullShots);
                    HashSet<Shot> allShots2 = findShotsInAll_Shots(connShots2, eachID, checkFullShots);
                    LOGGER.info("Shots in " + schemaShots1Alias + " for " + eachID + " with different data than in " + schemaShots2Alias + ":\n");
                    allShots1.stream().filter(eachShot -> !allShots2.contains(eachShot))
                            .forEach(eachShot -> LOGGER.info(eachShot.getUniqueShotId()));
                    LOGGER.info("Shots in " + schemaShots2Alias + " for " + eachID + " with different data than in " + schemaShots1Alias + ":\n");
                    allShots2.stream().filter(eachShot -> !allShots1.contains(eachShot))
                            .forEach(eachShot -> LOGGER.info(eachShot.getUniqueShotId()));
                    counter++;
                    if (counter % 1000 == 0) {
                        LOGGER.info(counter + "");
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
        if (schemaShots2Alias.equals("shottrustedindexed")) {
            schemaShots2Alias = "shottrusted";
            connShots2 = ScraperUtilsInterface.super.setNewConnection(schemaShots2Alias);
        }

    }

    /**
     * Finds all unique shot IDs for less intensive comparing
     *
     * @param conn      connection to database
     * @param tableName shot table name
     * @return set of all unique shot IDs
     */
    private HashSet<String> findShotIds(Connection conn, String tableName) {
        HashSet<String> uniqueShotIds = new HashSet<>();
        try {
            ResultSet dbResultSet = conn.prepareStatement("SELECT uniqueshotid FROM " + tableName).executeQuery();
            while (dbResultSet.next()) {
                uniqueShotIds.add(dbResultSet.getString("uniqueshotid"));
            }
            dbResultSet.close();
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
        return uniqueShotIds;
    }

    /**
     * Finds all shots (with full shot data for each shot, if demanded) for a given player ID in large shot table
     *
     * @param connShots      connection to shot database
     * @param playerId       current player ID
     * @param checkFullShots should get all data for every shot for comparing
     * @return set of all shots
     * @throws SQLException If query fails
     */
    private HashSet<Shot> findShotsInAll_Shots(Connection connShots, int playerId, boolean checkFullShots) throws SQLException {
        //Select only one player at a time to reduce memory impact
        //Don't get all columns if unnecessary
        ResultSet allShotsResultSet = connShots.prepareStatement("SELECT * FROM all_shots WHERE playerid = " + playerId).executeQuery();
        HashSet<Shot> allShots = new HashSet<>();
        while (allShotsResultSet.next()) {
            allShots.add(new Shot(
                    allShotsResultSet.getString("playerlast"),
                    allShotsResultSet.getString("playerfirst"),
                    allShotsResultSet.getString("season"),
                    allShotsResultSet.getString("seasontype"),
                    allShotsResultSet.getDate("calendar").toString(),
                    allShotsResultSet.getTime("clock").toString(),
                    allShotsResultSet.getString("shottype"),
                    allShotsResultSet.getString("playtype"),
                    allShotsResultSet.getString("teamname"),
                    allShotsResultSet.getString("awayteamname"),
                    allShotsResultSet.getString("hometeamname"),
                    allShotsResultSet.getString("shotzonebasic"),
                    allShotsResultSet.getString("shotzonearea"),
                    allShotsResultSet.getString("shotzonerange"),
                    allShotsResultSet.getInt("playerid"),
                    allShotsResultSet.getInt("gameid"),
                    allShotsResultSet.getInt("gameeventid"),
                    allShotsResultSet.getInt("minutes"),
                    allShotsResultSet.getInt("seconds"),
                    allShotsResultSet.getInt("x"),
                    allShotsResultSet.getInt("y"),
                    allShotsResultSet.getInt("distance"),
                    allShotsResultSet.getInt("make"),
                    allShotsResultSet.getInt("period"),
                    allShotsResultSet.getInt("teamid"),
                    allShotsResultSet.getInt("awayteamid"),
                    allShotsResultSet.getInt("hometeamid"),
                    allShotsResultSet.getInt("athome")));
        }
        allShotsResultSet.close();
        return allShots;
    }

    /**
     * Finds all relevant players for a given database and creates Player objects
     *
     * @param connPlayers connection to player database
     * @return set of all found Players
     */
    private HashSet<Player> getAllPlayers(Connection connPlayers) {
        HashSet<Player> playerHashSet = new HashSet<>();
        try {
            //Find all players
            ResultSet allPlayerResultSet = connPlayers.prepareStatement("SELECT * FROM player_relevant_data").executeQuery();
            while (allPlayerResultSet.next()) {
                try {
                    //Create Player object
                    Player player = new Player(allPlayerResultSet.getInt("id") + "", allPlayerResultSet.getString("lastname"), allPlayerResultSet.getString("firstname"),
                            allPlayerResultSet.getInt("currentlyactive") + "", allPlayerResultSet.getString("firstactiveyear").substring(0, 4), allPlayerResultSet.getString("mostrecentactiveyear").substring(0, 4));
                    String tableName = allPlayerResultSet.getString("lastname").replaceAll("[^A-Za-z0-9]", "") + "_" +
                            allPlayerResultSet.getString("firstname").replaceAll("[^A-Za-z0-9]", "")
                            + "_" + allPlayerResultSet.getInt("id") + "_individual_data";
                    //Find player activity in each year and update player object
                    ResultSet eachPlayerResultSet = connPlayers.prepareStatement("SELECT * FROM " + tableName).executeQuery();
                    while (eachPlayerResultSet.next()) {
                        player.addYearActivity(eachPlayerResultSet.getString("year"), eachPlayerResultSet.getInt("preseason"), eachPlayerResultSet.getInt("reg"), eachPlayerResultSet.getInt("playoffs"));
                    }
                    eachPlayerResultSet.close();
                    playerHashSet.add(player);
                } catch (Exception ex) {
                    LOGGER.error(ex.getMessage());
                }

            }
            allPlayerResultSet.close();
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
        return playerHashSet;
    }
}
