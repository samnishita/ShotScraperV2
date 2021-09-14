package com.example.ShotScraperV2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

    private Connection connShots1 = null, connPlayers1 = null, connShots2 = null, connPlayers2 = null;
    private String schemaShots1, locationShots1, schemaShots2, locationShots2, schemaPlayers1, locationPlayers1, schemaPlayers2, locationPlayers2;
    /**
     * Tables from original scraping algorithm that are verified as incorrect
     */
    private ArrayList<String> knownWrongShotTables = new ArrayList(Arrays.asList("Jenkins_John_203098_2014_15_Playoffs", "Ibaka_Serge_201586_2013_14_Playoffs", "Leaf_TJ_1628388_2019_20_Preseason",
            "Grant_Jerami_203924_2017_18_Playoffs", "Davis_Anthony_203076_2015_16_Preseason", "Leaf_TJ_1628388_2018_19_Preseason", "Connaughton_Pat_1626192_2019_20_RegularSeason",
            "Leonard_Kawhi_202695_2020_21_RegularSeason", "Blair_DeJuan_201971_2010_11_Preseason", "Johnson_BJ_1629168_2019_20_Preseason", "Harkless_Maurice_203090_2013_14_RegularSeason",
            "Karasev_Sergey_203508_2014_15_RegularSeason", "Johnson_Nick_203910_2014_15_Preseason", "Leaf_TJ_1628388_2017_18_RegularSeason", "Shamet_Landry_1629013_2020_21_Playoffs",
            "Leaf_TJ_1628388_2018_19_RegularSeason", "Jefferson_Richard_2210_2005_06_Playoffs", "Leaf_TJ_1628388_2017_18_Preseason", "Leaf_TJ_1628388_2018_19_Playoffs", "Leaf_TJ_1628388_2019_20_RegularSeason",
            "Ajinca_Alexis_201582_2015_16_Preseason", "Leaf_TJ_1628388_2020_21_RegularSeason", "Beverley_Patrick_201976_2015_16_RegularSeason", "Leaf_TJ_1628388_2020_21_Playoffs",
            "Johnson_BJ_1629168_2018_19_RegularSeason", "Johnson_BJ_1629168_2019_20_Playoffs", "Montross_Eric_376_1996_97_RegularSeason", "Johnson_BJ_1629168_2019_20_RegularSeason",
            "Alabi_Solomon_202374_2010_11_Preseason", "Grant_Jerami_203924_2016_17_Playoffs", "Black_Tarik_204028_2014_15_Preseason", "Ajinca_Alexis_201582_2014_15_Playoffs",
            "LeVert_Caris_1627747_2018_19_Preseason", "all_time_zoned_averages", "all_shot_types"));

    private HashSet<String> db1Rows = new HashSet<>(), db2Rows = new HashSet<>(), rowStrings = new HashSet<>(),
            rowsInDB1NotInDB2 = new HashSet<>(), rowsInDB2NotInDB1 = new HashSet<>(), entitiesInDB1NotInDB2 = new HashSet<>();
    private ResultSet dbResultSet;
    private StringBuilder eachRowStringBuilder = new StringBuilder(), entitiesInDB1NotInDB2Builder = new StringBuilder();
    private int tableCounter = 0;

    /**
     * Initializes DataDoubleChecker with database connections
     *
     * @param schemaShots1     first shot schema name
     * @param locationShots1   first shot location
     * @param schemaShots2     second shot schema name
     * @param locationShots2   second shot location
     * @param schemaPlayers1   first player schema name
     * @param locationPlayers1 first player location
     * @param schemaPlayers2   second player schema name
     * @param locationPlayers2 second player location
     */
    @Autowired
    public DataDoubleChecker(@Value("${shotschema1}") String schemaShots1,
                             @Value("${shotlocation1}") String locationShots1,
                             @Value("nbashotsnew") String schemaShots2,
                             @Value("${shotlocation2}") String locationShots2,
                             @Value("${playerschema1}") String schemaPlayers1,
                             @Value("${playerlocation1}") String locationPlayers1,
                             @Value("nbaplayerinfo") String schemaPlayers2,
                             @Value("${playerlocation2}") String locationPlayers2) {
        try {
            connShots1 = ScraperUtilsInterface.super.setNewConnection(schemaShots1, locationShots1);
            connPlayers1 = ScraperUtilsInterface.super.setNewConnection(schemaPlayers1, locationPlayers1);
            connShots2 = ScraperUtilsInterface.super.setNewConnection(schemaShots2, locationShots2);
            connPlayers2 = ScraperUtilsInterface.super.setNewConnection(schemaPlayers2, locationPlayers2);
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
            System.exit(1);
        }
        this.schemaShots1 = schemaShots1;
        this.locationShots1 = locationShots1;
        this.schemaShots2 = schemaShots2;
        this.locationShots2 = locationShots2;
        this.schemaPlayers1 = schemaPlayers1;
        this.locationPlayers1 = locationPlayers1;
        this.schemaPlayers2 = schemaPlayers2;
        this.locationPlayers2 = locationPlayers2;
    }

    /**
     * Compares all player tables by total table count and each table's data
     *
     * @param dropMismatchedTables should drop mismatched tables
     */
    public void comparePlayerTables(boolean dropMismatchedTables) {
        //Compare total table counts
        HashSet<String> playerTableNamesHash1 = getTableNames(connPlayers1, this.schemaPlayers1);
        HashSet<String> playerTableNamesHash2 = getTableNames(connPlayers2, this.schemaPlayers2);
        LOGGER.info("DB1 Table Count: " + playerTableNamesHash1.size());
        LOGGER.info("DB2 Table Count: " + playerTableNamesHash2.size());
        //Find tables in one database that aren't in the other
        getEntitiesInDB1NotInDB2(playerTableNamesHash1, playerTableNamesHash2, schemaPlayers1, schemaPlayers2, "Tables in " + schemaPlayers1 + " not in " + schemaPlayers2 + ":\n");
        getEntitiesInDB1NotInDB2(playerTableNamesHash2, playerTableNamesHash1, schemaPlayers2, schemaPlayers1, "Tables in " + schemaPlayers2 + " not in " + schemaPlayers1 + ":\n");
        //For tables existing in both databases, compare each row
        playerTableNamesHash1.stream()
                .filter(eachTableName -> playerTableNamesHash2.contains(eachTableName))
                .forEach(eachTableName -> comparePlayerTableData(eachTableName, dropMismatchedTables));
    }

    /**
     * Compares all shot tables by total table count and each table's data
     *
     * @param dropMismatchedTables should drop mismatched tables
     */
    public void compareShotTables(boolean dropMismatchedTables) {
        //Compare total table counts
        HashSet<String> shotTableNamesHash1 = getTableNames(connShots1, this.schemaShots1);
        HashSet<String> shotTableNamesHash2 = getTableNames(connShots2, this.schemaShots2);
        LOGGER.info("DB1 Table Count: " + shotTableNamesHash1.size());
        LOGGER.info("DB2 Table Count: " + shotTableNamesHash2.size());
        //Find tables in one database that aren't in the other
        getEntitiesInDB1NotInDB2(shotTableNamesHash1, shotTableNamesHash2, schemaShots1, schemaShots2, "Tables in " + schemaShots1 + " not in " + schemaShots2 + ":\n");
        getEntitiesInDB1NotInDB2(shotTableNamesHash2, shotTableNamesHash1, schemaShots2, schemaShots1, "Tables in " + schemaShots2 + " not in " + schemaShots1 + ":\n");
        //Large table split into multiple smaller queries
        compareAll_ShotsTable();
        //For tables existing in both databases, compare each row (not including all_shots and known wrong tables)
        shotTableNamesHash1.stream()
                .filter(eachTableName -> shotTableNamesHash2.contains(eachTableName))
                .filter(eachTableName -> !eachTableName.equals("all_shots"))
                .filter(eachTableName -> !knownWrongShotTables.contains(eachTableName))
                .forEach(eachTableName -> compareShotTableData(eachTableName, dropMismatchedTables));
    }

    /**
     * Retrieves all table names from the database
     *
     * @param conn   connection to the database
     * @param schema schema to be used
     * @return set of all tables
     */
    private HashSet<String> getTableNames(Connection conn, String schema) {
        HashSet<String> playerTableNamesHash = new HashSet<>();
        try {
            ResultSet playerTables = conn.getMetaData().getTables(schema, null, "%", null);
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
        entitiesInDB1NotInDB2.clear();
        for (String eachTableName : entity1) {
            if (!entity2.contains(eachTableName)) {
                entitiesInDB1NotInDB2.add(eachTableName);
            }
        }
        //Clear stringbuilder
        if (entitiesInDB1NotInDB2Builder.length() > 0) {
            entitiesInDB1NotInDB2Builder.delete(0, entitiesInDB1NotInDB2Builder.length());
            entitiesInDB1NotInDB2Builder.append(stringBuilderStart);
        }
        if (entitiesInDB1NotInDB2.size() == 0) {
            entitiesInDB1NotInDB2Builder.append("NONE");
        } else {
            entitiesInDB1NotInDB2.forEach(eachEntity -> entitiesInDB1NotInDB2Builder.append(eachEntity + "\n"));
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
     * Compares all rows in a given player table
     *
     * @param tableName  player table name
     * @param dropTables should drop tables if they are mismatched
     */
    private void comparePlayerTableData(String tableName, boolean dropTables) {
        //Analyze each row
        db1Rows.clear();
        db1Rows = createStringFromRow(connPlayers1, tableName);
        db2Rows.clear();
        db2Rows = createStringFromRow(connPlayers2, tableName);
        //Find which rows are different between the two sets
        rowsInDB1NotInDB2 = getEntitiesInDB1NotInDB2(db1Rows, db2Rows, this.schemaPlayers1, this.schemaPlayers2, "Rows found in " + this.schemaPlayers1 + "." + tableName + " not found in " + this.schemaPlayers2 + "." + tableName + ": \n");
        rowsInDB2NotInDB1 = getEntitiesInDB1NotInDB2(db2Rows, db1Rows, this.schemaPlayers2, this.schemaPlayers1, "Rows found in " + this.schemaPlayers2 + "." + tableName + " not found in " + this.schemaPlayers1 + "." + tableName + ": \n");
        //If a difference between sets exists
        if ((rowsInDB1NotInDB2.size() != 0 || rowsInDB2NotInDB1.size() != 0) && rowsInDB1NotInDB2.size() == rowsInDB2NotInDB1.size()) {
            if (rowsInDB1NotInDB2.size() == 1) {
                //Lots of 2019-20 preseasons are missing from the original scrape
                if (rowsInDB1NotInDB2.stream().filter(eachRow -> !eachRow.contains("2019-20")).count() > 0) {
                    LOGGER.info("MISMATCHED TABLE: " + tableName);
                    if (dropTables) {
                        dropMismatchedTables(tableName, connPlayers1, schemaPlayers1);
                    }
                }
            } else {
                LOGGER.info("MISMATCHED TABLE: " + tableName);
                if (dropTables) {
                    dropMismatchedTables(tableName, connPlayers1, schemaPlayers1);
                }
            }
        }
        tableCounter++;
        if (tableCounter % 1000 == 0) {
            LOGGER.info(tableCounter + "");
        }
    }

    /**
     * Compares all rows in a given shot table
     *
     * @param tableName  shot table name
     * @param dropTables should drop tables if they are mismatched
     */
    private void compareShotTableData(String tableName, boolean dropTables) {
        //Analyze each row
        db1Rows.clear();
        db1Rows = createStringFromRow(connShots1, tableName);
        db2Rows.clear();
        db2Rows = createStringFromRow(connShots2, tableName);
        rowsInDB1NotInDB2.clear();
        rowsInDB1NotInDB2 = getEntitiesInDB1NotInDB2(db1Rows, db2Rows, this.schemaShots1, this.schemaShots2, "Rows found in " + this.schemaShots1 + "." + tableName + " not found in " + this.schemaShots2 + "." + tableName + ": \n");
        rowsInDB2NotInDB1.clear();
        rowsInDB2NotInDB1 = getEntitiesInDB1NotInDB2(db2Rows, db1Rows, this.schemaShots2, this.schemaShots1, "Rows found in " + this.schemaShots2 + "." + tableName + " not found in " + this.schemaShots1 + "." + tableName + ": \n");
        //Find which rows are different between the two sets
        if ((rowsInDB1NotInDB2.size() != 0 || rowsInDB2NotInDB1.size() != 0) && !knownWrongShotTables.contains(tableName)) {
            LOGGER.info("MISMATCHED TABLE: " + tableName);
            if (dropTables) {
                dropMismatchedTables(tableName, connShots1, schemaShots1);
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
        rowStrings.clear();
        try {
            dbResultSet = conn.prepareStatement("SELECT * FROM " + tableName).executeQuery();
            while (dbResultSet.next()) {
                //Clear stringbuilder
                if (eachRowStringBuilder.length() > 0) {
                    eachRowStringBuilder.delete(0, eachRowStringBuilder.length());
                }
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
     * @param tableName table to drop
     * @param conn      connection to database
     * @param schema    database schema
     */
    private void dropMismatchedTables(String tableName, Connection conn, String schema) {
        try {
            LOGGER.info("Dropping " + tableName + " from " + schema);
            conn.prepareStatement("DROP TABLE `" + schema + "`.`" + tableName + "`").execute();
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    /**
     * Compares large table by doing multiple smaller comparisons
     */
    private void compareAll_ShotsTable() {
        try {
            //Gather all player IDs
            HashSet<Integer> allIds = new HashSet<>();
            ResultSet conn1Ids = connPlayers1.prepareStatement("SELECT id FROM player_relevant_data").executeQuery();
            while (conn1Ids.next()) {
                allIds.add(conn1Ids.getInt("id"));
            }
            conn1Ids.close();
            HashSet<String> allShots1 = new HashSet<>(), allShots2 = new HashSet<>();
            ResultSet allShotsResultSet;
            StringBuilder shotBuilder = new StringBuilder();
            int counter = 0;
            for (Integer eachID : allIds) {
                allShots1.clear();
                allShots2.clear();
                //Only compare shots from a single player at a time
                allShotsResultSet = connShots1.prepareStatement("SELECT * FROM all_shots WHERE playerid = " + eachID).executeQuery();
                while (allShotsResultSet.next()) {
                    if (shotBuilder.length() > 0) {
                        shotBuilder.delete(0, shotBuilder.length());
                    }
                    for (int i = 1; i <= allShotsResultSet.getMetaData().getColumnCount(); i++) {
                        shotBuilder.append(allShotsResultSet.getString(i));
                        if (i != allShotsResultSet.getMetaData().getColumnCount()) {
                            shotBuilder.append("_");
                        }
                    }
                    allShots1.add(shotBuilder.toString());
                }
                allShotsResultSet.close();
                allShotsResultSet = connShots2.prepareStatement("SELECT * FROM all_shots WHERE playerid = " + eachID).executeQuery();
                while (allShotsResultSet.next()) {
                    if (shotBuilder.length() > 0) {
                        shotBuilder.delete(0, shotBuilder.length());
                    }
                    for (int i = 1; i <= allShotsResultSet.getMetaData().getColumnCount(); i++) {
                        shotBuilder.append(allShotsResultSet.getString(i));
                        if (i != allShotsResultSet.getMetaData().getColumnCount()) {
                            shotBuilder.append("_");
                        }
                    }
                    allShots2.add(shotBuilder.toString());
                }
                allShotsResultSet.close();
                getEntitiesInDB1NotInDB2(allShots1, allShots2, schemaShots1, schemaShots2, "Rows for ID " + eachID + "found in " + this.schemaShots1 + ".all_shots not found in " + this.schemaShots2 + ".all_shots: \n");
                getEntitiesInDB1NotInDB2(allShots2, allShots1, schemaShots2, schemaShots1, "Rows for ID " + eachID + "found in " + this.schemaShots2 + ".all_shots not found in " + this.schemaShots1 + ".all_shots: \n");
                counter++;
                if (counter % 1000 == 0) {
                    LOGGER.info(counter + "");
                }
            }
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }

    }
}
