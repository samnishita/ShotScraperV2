package com.example.ShotScraperV2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/**
 * Database updater for calculating shot averages and organizing data
 */
@Component
public class DatabaseUpdater implements ScraperUtilsInterface {
    private Logger LOGGER = LoggerFactory.getLogger(DatabaseUpdater.class);
    private Connection connShots1 = null, connPlayers1 = null, connShots2 = null, connPlayers2 = null;
    private String schemaShots1, locationShots1, schemaShots2, locationShots2, schemaPlayers1, locationPlayers1, schemaPlayers2, locationPlayers2;

    /**
     * Initializes Database updater with database connections
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
    public DatabaseUpdater(@Value("${shotschema1}") String schemaShots1,
                           @Value("${shotlocation1}") String locationShots1,
                           @Value("${shotschema2}") String schemaShots2,
                           @Value("${shotlocation2}") String locationShots2,
                           @Value("${playerschema1}") String schemaPlayers1,
                           @Value("${playerlocation1}") String locationPlayers1,
                           @Value("${playerschema2}") String schemaPlayers2,
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
     * Saves a list of all players active for each year
     *
     * @param connPlayers connection to player database
     * @throws SQLException If SQL queries fail
     */
    public void organizeByYear(Connection connPlayers) throws SQLException {
        //Create tables for active players for each year starting at 1996-97
        String yearString = "1996", fullYear, tableName;
        while (Integer.parseInt(yearString.substring(0, 4)) <= Integer.parseInt(ScraperUtilsInterface.super.getCurrentYear().substring(0, 4))) {
            fullYear = ScraperUtilsInterface.super.buildYear(yearString);
            tableName = fullYear.substring(0, 4) + "_" + fullYear.substring(5, 7) + "_active_players";
            String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
                    + "  `id` int NOT NULL,\n"
                    + "  `lastname` varchar(25) NOT NULL,\n"
                    + "  `firstname` varchar(25) DEFAULT NULL,\n"
                    + "  PRIMARY KEY (`id`),\n"
                    + "  UNIQUE KEY `id_UNIQUE` (`id`)\n"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
            connPlayers.prepareStatement(createTable).execute();
            yearString = (Integer.parseInt(yearString) + 1) + "";
        }
        ResultSet allPlayers = connPlayers.prepareStatement("SELECT * FROM player_relevant_data").executeQuery();
        String firstName, lastName, sqlInsert, sqlIndiv;
        int id;
        ResultSet rsInd;
        //Iterate through all players and add them to the tables corresponding to years they were active
        while (allPlayers.next()) {
            firstName = allPlayers.getString("firstname");
            lastName = allPlayers.getString("lastname");
            id = allPlayers.getInt("id");
            try {
                sqlIndiv = "SELECT * FROM " + lastName.replaceAll("[^A-Za-z0-9]", "") + "_" + firstName.replaceAll("[^A-Za-z0-9]", "") + "_" + id + "_individual_data";
                rsInd = connPlayers.prepareStatement(sqlIndiv).executeQuery();
                while (rsInd.next()) {
                    if (rsInd.getInt("reg") == 1 || rsInd.getInt("preseason") == 1 || rsInd.getInt("playoffs") == 1) {
                        try {
                            sqlInsert = "INSERT INTO " + rsInd.getString("year").substring(0, 4) + "_" + rsInd.getString("year").substring(5, 7)
                                    + "_active_players (id,lastname,firstname) VALUES (" + id + ", \"" + lastName + "\", \"" + firstName + "\")";
                            connPlayers.prepareStatement(sqlInsert).execute();
                            LOGGER.info("Inserted " + firstName + " " + lastName + " (" + id + ") into " + rsInd.getString("year").substring(0, 4)
                                    + "_" + rsInd.getString("year").substring(5, 7) + "_active_players");
                        } catch (SQLIntegrityConstraintViolationException ex) {

                        } catch (Exception ex) {
                            LOGGER.error(ex.getMessage());
                        }
                    }
                }
            } catch (SQLSyntaxErrorException ex) {
                LOGGER.error(ex.getMessage());
            } catch (SQLException ex) {
                LOGGER.error(ex.getMessage());
            }
        }
    }

    /**
     * Updates the misc parameters
     *
     * @param newValue    the new value
     * @param connPlayers connection to player database
     * @param type        the existing key
     * @throws SQLException If the updating fails
     */
    public void updateMisc(String newValue, Connection connPlayers, String type) throws SQLException {
        connPlayers.prepareStatement("UPDATE misc SET value = '" + newValue + "' WHERE type = '" + type + "'").execute();
    }

    /**
     * Calculates the shot percentage for uniformly spaced areas of the court with a given size
     *
     * @param year      an optional year
     * @param offset    the size of each area
     * @param connShots connection to shots database
     * @throws SQLException If querying the database fails
     */
    public void createShotLocationAverages(String year, int offset, Connection connShots) throws SQLException {
        String tableName;
        String sql = "SELECT x,y,make FROM all_shots";
        //Generate table name
        if (year.equals("")) {
            tableName = "all_time_location_averages_offset_" + offset;
        } else {
            tableName = ScraperUtilsInterface.super.buildYear(year + "").replace("-", "_") + "_location_averages_offset_" + offset;
            sql = sql + " WHERE season = '" + ScraperUtilsInterface.super.buildYear(year) + "'";
        }
        LOGGER.info("tableName: " + tableName);
        String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n" +
                "  `uniqueid` varchar(15) NOT NULL,\n" +
                "  `xmin` int NOT NULL,\n" +
                "  `ymin` int NOT NULL,\n" +
                "  `shotcount` int NOT NULL,\n" +
                "  `average` decimal(7,4) NOT NULL,\n" +
                "  PRIMARY KEY (`uniqueid`),\n" +
                "  UNIQUE KEY `" + tableName + "_UN` (`uniqueid`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
        connShots.prepareStatement(sqlCreateTable).execute();
        //Create map of each area
        LinkedHashMap<String, double[]> nbaAverages = new LinkedHashMap();
        for (int j = -55; j < 400; j = j + offset) {
            for (int i = -250; i < 250; i = i + offset) {
                nbaAverages.put(i + "_" + j, new double[]{0.0, 0.0, 0.0});
            }
        }
        try {
            int x, y;
            ResultSet rs = connShots.prepareStatement(sql).executeQuery();
            //For each shot
            while (rs.next()) {
                //Skip shots too far from the basket
                if (rs.getInt("y") >= 400) {
                    continue;
                }
                //Account for overflow if the court width is not perfectly divisible by the offset
                x = ((rs.getInt("x") + 250) / offset) * offset - 250;
                if (x >= 250) {
                    x = ((rs.getInt("x") + 250) / offset) * offset - 250 - offset;
                }
                y = ((rs.getInt("y") + 55) / offset) * offset - 55;
                //Update shot count and makes for the current shot's area
                try {
                    nbaAverages.get(x + "_" + y)[1] = nbaAverages.get(x + "_" + y)[1] + 1;
                    if (rs.getInt("make") == 1) {
                        nbaAverages.get(x + "_" + y)[0] = nbaAverages.get(x + "_" + y)[0] + 1;
                    }
                } catch (SQLException ex) {
                    LOGGER.error(ex.getMessage());
                }
            }
            rs.close();
            PreparedStatement stmt;
            String sqlInsert = "INSERT INTO " + tableName + " VALUES(?,?,?,?,?)";
            String[] xySplit;
            //Calculate the average for each area and save into database
            for (String eachCoordinate : nbaAverages.keySet()) {
                xySplit = eachCoordinate.split("_");
                stmt = connShots.prepareStatement(sqlInsert);
                stmt.setString(1, "(" + xySplit[0] + "," + xySplit[1] + ")");
                stmt.setInt(2, Integer.parseInt(xySplit[0]));
                stmt.setInt(3, Integer.parseInt(xySplit[1]));
                if (nbaAverages.get(eachCoordinate)[1] != 0) {
                    nbaAverages.get(eachCoordinate)[2] = nbaAverages.get(eachCoordinate)[0] * 1.0 / nbaAverages.get(eachCoordinate)[1];
                }
                stmt.setInt(4, (int) nbaAverages.get(eachCoordinate)[1]);
                stmt.setBigDecimal(5, new BigDecimal(nbaAverages.get(eachCoordinate)[2]));
                stmt.execute();
            }
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    /**
     * Gets connection to the first shot database
     *
     * @return connection to the first shot database
     */
    public Connection getConnShots1() {
        return connShots1;
    }

    /**
     * Gets connection to the second shot database
     *
     * @return connection to the second shot database
     */
    public Connection getConnShots2() {
        return connShots2;
    }

    /**
     * Gets connection to the first player database
     *
     * @return connection to the first player database
     */
    public Connection getConnPlayers1() {
        return connPlayers1;
    }

    /**
     * Gets connection to the second player database
     *
     * @return connection to the second player database
     */
    public Connection getConnPlayers2() {
        return connPlayers2;
    }

    /**
     * Calculates the shot percentages for each zone on the court
     *
     * @param year      an optional year
     * @param connShots connection to shots database
     * @throws SQLException If querying the database fails
     */
    public void getZonedAverages(String year, Connection connShots) throws SQLException {
        String tableName = year.equals("") ? "all_time_zoned_averages" : ScraperUtilsInterface.super.buildYear(year).replace("-", "_") + "_zoned_averages";
        connShots.prepareStatement("CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
                + "  `uniqueid` int NOT NULL,\n"
                + "  `shotcount` int NOT NULL,\n"
                + "  `average` decimal(7,4) NOT NULL,\n"
                + "  UNIQUE KEY `" + tableName + "_UN` (`uniqueid`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci").execute();
        LOGGER.info(tableName);
        HashMap<Integer, Double[]> allZones = new HashMap();
        for (int i = 1; i < 16; i++) {
            allZones.put(i, new Double[]{0.0, 0.0, 0.0});
        }
        String sqlSelect = "SELECT make,shotzonebasic,shotzonearea,shotzonerange FROM all_shots ";
        if (!year.equals("")) {
            sqlSelect = sqlSelect + " WHERE season = '" + ScraperUtilsInterface.super.buildYear(year) + "'";
        }
        ResultSet rs = connShots.prepareStatement(sqlSelect).executeQuery();
        //Each area can include many defined zones
        while (rs.next()) {
            switch (rs.getString("shotzonebasic")) {
                case "Backcourt":
                    break;
                case "Restricted Area":
                    addShotToHashMap(allZones, 1, rs.getInt("make"));
                    break;
                case "In The Paint (Non-RA)":
                    switch (rs.getString("shotzonearea")) {
                        case "Left Side(L)":
                            switch (rs.getString("shotzonerange")) {
                                case "8-16 ft.":
                                    addShotToHashMap(allZones, 3, rs.getInt("make"));
                                    break;
                            }
                            break;
                        case "Center(C)":
                            switch (rs.getString("shotzonerange")) {
                                case "Less Than 8 ft.":
                                    addShotToHashMap(allZones, 2, rs.getInt("make"));
                                    break;
                                case "8-16 ft.":
                                    addShotToHashMap(allZones, 4, rs.getInt("make"));
                                    break;

                            }
                            break;
                        case "Right Side(R)":
                            switch (rs.getString("shotzonerange")) {
                                case "8-16 ft.":
                                    addShotToHashMap(allZones, 5, rs.getInt("make"));
                                    break;
                            }
                            break;
                    }
                    break;
                case "Mid-Range":
                    switch (rs.getString("shotzonearea")) {
                        case "Left Side(L)":
                            switch (rs.getString("shotzonerange")) {
                                case "8-16 ft.":
                                    addShotToHashMap(allZones, 3, rs.getInt("make"));
                                    break;
                                case "16-24 ft.":
                                    addShotToHashMap(allZones, 6, rs.getInt("make"));
                                    break;
                            }
                            break;
                        case "Left Side Center(LC)":
                            switch (rs.getString("shotzonerange")) {
                                case "16-24 ft.":
                                    addShotToHashMap(allZones, 7, rs.getInt("make"));
                                    break;
                            }
                            break;
                        case "Center(C)":
                            switch (rs.getString("shotzonerange")) {
                                case "8-16 ft.":
                                    addShotToHashMap(allZones, 4, rs.getInt("make"));
                                    break;
                                case "16-24 ft.":
                                    addShotToHashMap(allZones, 8, rs.getInt("make"));
                                    break;
                            }
                            break;
                        case "Right Side Center(RC)":
                            switch (rs.getString("shotzonerange")) {
                                case "16-24 ft.":
                                    addShotToHashMap(allZones, 9, rs.getInt("make"));
                                    break;
                            }
                            break;
                        case "Right Side(R)":
                            switch (rs.getString("shotzonerange")) {
                                case "8-16 ft.":
                                    addShotToHashMap(allZones, 5, rs.getInt("make"));
                                    break;
                                case "16-24 ft.":
                                    addShotToHashMap(allZones, 10, rs.getInt("make"));
                                    break;
                            }
                            break;
                    }
                    break;
                case "Left Corner 3":
                    addShotToHashMap(allZones, 11, rs.getInt("make"));
                    break;
                case "Right Corner 3":
                    addShotToHashMap(allZones, 15, rs.getInt("make"));
                    break;
                case "Above the Break 3":
                    switch (rs.getString("shotzonearea")) {
                        case "Left Side Center(LC)":
                            switch (rs.getString("shotzonerange")) {
                                case "24+ ft.":
                                    addShotToHashMap(allZones, 12, rs.getInt("make"));
                                    break;
                            }
                            break;
                        case "Center(C)":
                            switch (rs.getString("shotzonerange")) {
                                case "24+ ft.":
                                    addShotToHashMap(allZones, 13, rs.getInt("make"));
                                    break;
                            }
                            break;
                        case "Right Side Center(RC)":
                            switch (rs.getString("shotzonerange")) {
                                case "24+ ft.":
                                    addShotToHashMap(allZones, 14, rs.getInt("make"));
                                    break;
                                default:
                            }
                            break;
                    }
                    break;
            }
        }
        rs.close();
        //Calculate the zone percentages and save into database
        for (Integer each : allZones.keySet()) {
            allZones.get(each)[2] = allZones.get(each)[0] * 1.0 / allZones.get(each)[1];
            try {
                PreparedStatement stmt = connShots.prepareStatement("INSERT INTO " + tableName + " VALUES (?,?,?)");
                stmt.setInt(1, each);
                stmt.setInt(2, allZones.get(each)[1].intValue());
                stmt.setBigDecimal(3, new BigDecimal(allZones.get(each)[2]));
                stmt.execute();
            } catch (Exception ex) {
                LOGGER.error(ex.getMessage());
            }
        }
    }

    /**
     * Saves a shot to a hashmap and checks for a made shot
     *
     * @param hashmap  the hashmap to be added to
     * @param selector the hashmap key
     * @param make     the shot make parameter
     */
    private void addShotToHashMap(HashMap<Integer, Double[]> hashmap, int selector, int make) {
        hashmap.get(selector)[1] = hashmap.get(selector)[1] + 1;
        if (make == 1) {
            hashmap.get(selector)[0] = hashmap.get(selector)[0] + 1;
        }
    }

    /**
     * Finds all existing shot types and saves to database
     *
     * @param connShots connection to shots database
     * @throws SQLException If querying database fails
     */
    public void createPlayTypeTable(Connection connShots) throws SQLException {
        String sqlCreateTable = "CREATE TABLE IF NOT EXISTS all_shot_types (\n"
                + "	playtype varchar(100) NOT NULL,\n"
                + "	CONSTRAINT allplaytypes_UN UNIQUE KEY (playtype)\n"
                + ")\n"
                + "ENGINE=InnoDB\n"
                + "DEFAULT CHARSET=utf8mb4\n"
                + "COLLATE=utf8mb4_0900_ai_ci;";
        connShots.prepareStatement(sqlCreateTable).execute();
        String sqlSelect = "SELECT playtype from all_shots";
        HashSet<String> playTypes = new HashSet();
        ResultSet rs = connShots.prepareStatement(sqlSelect).executeQuery();
        while (rs.next()) {
            playTypes.add(rs.getString("playtype").replace("shot", "Shot"));
        }
        ArrayList<String> list = new ArrayList();
        playTypes.forEach(eachPlayType -> list.add(eachPlayType));
        //Filter out "No Shot" (Why does that even exist?)
        list.stream().filter(eachShotType -> !eachShotType.equals("No Shot")).sorted().forEach(eachShotType -> {
            try {
                connShots.prepareStatement("INSERT INTO all_shot_types VALUES ('" + eachShotType + "')").execute();
            } catch (Exception ex) {
                LOGGER.error(ex.getMessage());
            }
        });
    }

    /**
     * Updates list of all players who were active during current year
     *
     * @param connPlayers connection to player database
     * @throws SQLException If querying the database fails
     */
    public void updateActivePlayersListForCurrentYear(Connection connPlayers) throws SQLException {
        HashSet<Integer> activePlayersInCurrentYear = new HashSet();
        //Find players already in the correct list
        ResultSet existingActivePlayersResultSet = connPlayers.prepareStatement("SELECT id FROM "
                + ScraperUtilsInterface.super.getCurrentYear().substring(0, 4) + "_" + ScraperUtilsInterface.super.getCurrentYear().substring(5, 7) + "_active_players").executeQuery();
        while (existingActivePlayersResultSet.next()) {
            activePlayersInCurrentYear.add(existingActivePlayersResultSet.getInt("id"));
        }
        ResultSet allPlayers = connPlayers.prepareStatement("SELECT id,lastname,firstname FROM player_relevant_data WHERE mostrecentactiveyear = '" + ScraperUtilsInterface.super.getCurrentYear() + "'").executeQuery();
        String firstName, lastName, sqlInsert, sqlIndiv;
        int id;
        ResultSet rsInd;
        while (allPlayers.next()) {
            firstName = allPlayers.getString("firstname");
            lastName = allPlayers.getString("lastname");
            id = allPlayers.getInt("id");
            //Find players not yet in the list
            if (activePlayersInCurrentYear.contains(id)) {
                continue;
            }
            try {
                sqlIndiv = "SELECT * FROM " + lastName.replaceAll("[^A-Za-z0-9]", "") + "_" + firstName.replaceAll("[^A-Za-z0-9]", "") + "_"
                        + id + "_individual_data WHERE year = '" + ScraperUtilsInterface.super.getCurrentYear() + "'";
                rsInd = connPlayers.prepareStatement(sqlIndiv).executeQuery();
                while (rsInd.next()) {
                    if ((rsInd.getInt("reg") == 1 || rsInd.getInt("preseason") == 1 || rsInd.getInt("playoffs") == 1) && !activePlayersInCurrentYear.contains(id)) {
                        try {
                            sqlInsert = "INSERT INTO " + rsInd.getString("year").substring(0, 4) + "_" + rsInd.getString("year").substring(5, 7)
                                    + "_active_players (id,lastname,firstname) VALUES (" + id + ", \"" + lastName + "\", \"" + firstName + "\")";
                            connPlayers.prepareStatement(sqlInsert).execute();
                            LOGGER.info("Inserted " + firstName + " " + lastName + " (" + id + ") into " + rsInd.getString("year").substring(0, 4)
                                    + "_" + rsInd.getString("year").substring(5, 7) + "_active_players");
                        } catch (SQLIntegrityConstraintViolationException ex) {

                        } catch (Exception ex) {
                            LOGGER.error(ex.getMessage());
                        }
                    }
                }
            } catch (SQLSyntaxErrorException ex) {

            } catch (SQLException ex) {
                LOGGER.error(ex.getMessage());
            }
        }
    }

    /**
     * Calculates the shot percentage at every distance
     *
     * @param year      an optional year
     * @param connShots connection to shots database
     */
    public void getDistancesAndAvg(String year, Connection connShots) {
        String tableName = year.equals("") ? "all_time_distance_averages" : ScraperUtilsInterface.super.buildYear(year).replace("-", "_") + "_distance_averages";
        String sqlSelect = "SELECT distance,make FROM all_shots";
        LOGGER.info(tableName);
        if (!year.equals("")) {
            sqlSelect = sqlSelect + " WHERE season = '" + ScraperUtilsInterface.super.buildYear(year) + "'";
        }
        try {
            //Get all shots
            ResultSet rs = connShots.prepareStatement(sqlSelect).executeQuery();
            HashMap<Integer, Double[]> mapDistToAvg = new HashMap<>();
            for (int i = 0; i < 90; i++) {
                mapDistToAvg.put(i, new Double[]{0.0, 0.0, 0.0});
            }
            int distance, make;
            while (rs.next()) {
                distance = rs.getInt("distance");
                make = rs.getInt("make");
                mapDistToAvg.get(distance)[1] = mapDistToAvg.get(distance)[1] + 1;
                if (make == 1) {
                    mapDistToAvg.get(distance)[0] = mapDistToAvg.get(distance)[0] + 1;
                }
            }
            rs.close();
            String createTable = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" +
                    "\tdistance INT NOT NULL,\n" +
                    "\tshotcount INT NOT NULL,\n" +
                    "\taverage DECIMAL(10,4) NOT NULL,\n" +
                    "\tCONSTRAINT " + tableName + "_UN UNIQUE KEY (distance)\n" +
                    ")\n" +
                    "ENGINE=InnoDB\n" +
                    "DEFAULT CHARSET=utf8mb4\n" +
                    "COLLATE=utf8mb4_0900_ai_ci;";
            connShots.prepareStatement(createTable).execute();
            mapDistToAvg.keySet().forEach(each -> {
                mapDistToAvg.get(each)[2] = mapDistToAvg.get(each)[0] / mapDistToAvg.get(each)[1];
                try {
                    PreparedStatement stmt = connShots.prepareStatement("INSERT INTO " + tableName + " VALUES (?,?,?)");
                    stmt.setInt(1, each);
                    stmt.setInt(2, mapDistToAvg.get(each)[1].intValue());
                    stmt.setInt(3, mapDistToAvg.get(each)[2].intValue());
                    stmt.execute();
                } catch (SQLException ex) {
                    LOGGER.error(ex.getMessage());
                }
            });
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
    }
}
