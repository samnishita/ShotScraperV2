package com.example.ShotScraperV2;

import com.example.ShotScraperV2.nbaobjects.Player;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Scraper of all shots
 */
public class ShotScraper implements ScraperUtilsInterface {
    private final Logger LOGGER = LoggerFactory.getLogger(ShotScraper.class);
    private String schemaShots1Alias, schemaShots2Alias, schemaPlayers1Alias, schemaPlayers2Alias;
    /**
     * Map of column name and the normal readable version of the season type
     */
    private final Map<String, String> mapDBColumnToSeasonType = new HashMap<>(Map.ofEntries(
            Map.entry("preseason", "Preseason"),
            Map.entry("reg", "Regular Season"),
            Map.entry("playoffs", "Playoffs")));

    /**
     * Map of normal season type and the version accepted by the URL parameters
     */
    private final Map<String, String> mapDBColumnToURLParamName = new HashMap<>(Map.ofEntries(
            Map.entry("Preseason", "Pre+Season"),
            Map.entry("Regular Season", "Regular+Season"),
            Map.entry("Playoffs", "Playoffs")));
    /**
     * Map of team abbreviation and team id
     */
    private HashMap<String, Integer> teamAbbrMap;

    /**
     * Map of data location in shot data response to prepared statement index
     * <p></p>
     * Helps organize data in database in more logical order
     */
    private final HashMap<Integer, Integer> MAP_JSON_ARRAY_INDEX_TO_PS_INDEX = new HashMap<>(Map.ofEntries(
            Map.entry(1, 7),//gameID
            Map.entry(2, 8),//gameEventID
            Map.entry(3, 2),//playerID
            Map.entry(5, 20),//teamID
            Map.entry(6, 21),//teamName
            Map.entry(7, 17),//period
            Map.entry(8, 11),//minutes
            Map.entry(9, 12),//seconds
            Map.entry(10, 16),//make
            Map.entry(11, 19),//playType
            Map.entry(12, 18),//shotType
            Map.entry(13, 27),//shotZoneBasic
            Map.entry(14, 28),//shotZoneArea
            Map.entry(15, 29),//shotZoneRange
            Map.entry(16, 15),//distance
            Map.entry(17, 13),//x
            Map.entry(18, 14),//y
            Map.entry(21, 9),//calendar
            Map.entry(22, 25),//homeTeamName
            Map.entry(23, 23)//awayTeamName
    ));

    /**
     * Map of old team abbreviations to their updated team abbreviations
     */
    private final Map<String, String> specialTeams = Map.ofEntries(Map.entry("NJN", "BKN"),
            Map.entry("VAN", "MEM"),
            Map.entry("NOK", "NOP"),
            Map.entry("NOH", "NOP"),
            Map.entry("SEA", "OKC"),
            Map.entry("CHH", "CHA"));
    private int totalNewShotsAdded;
    private IndividualPlayerScraper individualPlayerScraper;

    /**
     * Initializes ShotScraper with database connections
     *
     * @param schemaShots1Alias       first shot schema alias
     * @param schemaShots2Alias       second shot schema alias
     * @param schemaPlayers1Alias     first player schema alias
     * @param schemaPlayers2Alias     second player schema alias
     * @param individualPlayerScraper player scraper
     */
    public ShotScraper(String schemaShots1Alias, String schemaShots2Alias, String schemaPlayers1Alias, String schemaPlayers2Alias, IndividualPlayerScraper individualPlayerScraper) {
        try {
            this.schemaShots1Alias = schemaShots1Alias;
            this.schemaShots2Alias = schemaShots2Alias;
            this.schemaPlayers1Alias = schemaPlayers1Alias;
            this.schemaPlayers2Alias = schemaPlayers2Alias;
            Connection connPlayers = ScraperUtilsInterface.super.setNewConnection(schemaPlayers1Alias);
            Connection connShots1 = ScraperUtilsInterface.super.setNewConnection(schemaShots1Alias);
            Connection connShots2 = ScraperUtilsInterface.super.setNewConnection(schemaShots2Alias);
            this.teamAbbrMap = createTeamAbbreviationMap(connPlayers);
            createAllShotsTable(connShots1, connShots2);
            connPlayers.close();
            connShots1.close();
            connShots2.close();
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
        totalNewShotsAdded = 0;
        this.individualPlayerScraper = individualPlayerScraper;
    }

    /**
     * Generates a map with (K,V) of (team abbreviation, team ID)
     *
     * @param connPlayers connection to player database (where team data is stored)
     * @return hashmap of team abbreviation and team ID
     * @throws SQLException If SQL query fails
     */
    private HashMap<String, Integer> createTeamAbbreviationMap(Connection connPlayers) throws SQLException {
        HashMap<String, Integer> map = new HashMap<>();
        ResultSet rsTeam = connPlayers.prepareStatement("SELECT * FROM team_data").executeQuery();
        while (rsTeam.next()) {
            map.put(rsTeam.getString("abbr"), rsTeam.getInt("id"));
        }
        rsTeam.close();
        return map;
    }

    /**
     * Finds player activity from their individual data table
     *
     * @param lastName          player last name
     * @param firstName         player first name
     * @param playerID          player ID
     * @param onlyCurrentSeason if searching only for player activity in the current year and season type
     * @param currentSeasonType the current season type (used when onlyCurrentSeason == true)
     * @param connPlayers       connection to player database
     * @return ResultSet of all rows fulfilling the SQL query
     */
    protected ResultSet findPlayerActivity(String lastName, String firstName, int playerID, boolean onlyCurrentSeason, String currentSeasonType, Connection connPlayers) {
        //Get individual player activity data
        try {
            String indivSelect = "SELECT * FROM " + lastName + "_" + firstName + "_" + playerID + "_individual_data";
            //If only updating current season, add WHERE clause for current year and desired season type
            if (onlyCurrentSeason) {
                indivSelect = indivSelect + " WHERE year = '" + ScraperUtilsInterface.super.getCurrentYear() + "' AND " + currentSeasonType + " = 1";
            }
            return connPlayers.prepareStatement(indivSelect).executeQuery();
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
        return null;
    }

    /**
     * Finds all existing shots in a given shot table
     *
     * @param connShots             connection to shots database
     * @param shotTableName         name of shot table to be searched
     * @param existingUniqueShotIds set of unique shot IDs to be updated
     */
    protected void findExistingShots(Connection connShots, String shotTableName, HashSet<String> existingUniqueShotIds) {
        //Find existing shots to reduce number of statements to be executed
        try {
            ResultSet existingShotsResultSet = connShots.prepareStatement("SELECT uniqueshotid FROM " + shotTableName).executeQuery();
            while (existingShotsResultSet.next()) {
                existingUniqueShotIds.add(existingShotsResultSet.getString("uniqueshotid"));
            }
            existingShotsResultSet.close();
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    /**
     * Finds if given shot table exists in given schema
     *
     * @param connShots     connection to shot database
     * @param schema        database schema
     * @return number of tables found
     */
    protected HashSet<String> findExistingTables(Connection connShots, String schema) {
        HashSet<String> existingTables = new HashSet<>();
        try {
            ResultSet shotTablesRS = connShots.getMetaData().getTables(schema, null, "%", new String[]{"TABLE"});
            while (shotTablesRS.next()) {
                existingTables.add(shotTablesRS.getString(3));
            }
            shotTablesRS.close();
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
        return existingTables;
    }

    /**
     * Scrapes all shots for all players
     *
     * @param connPlayers1      connection to first player database
     * @param connPlayers2      connection to second player database
     * @param connShots1        connection to first shot database
     * @param connShots2        connection to second shot database
     * @param onlyCurrentSeason should only search for current year and season type
     * @param currentSeasonType current season type
     */
    public void getEveryShotWithMainThread(Connection connPlayers1, Connection connPlayers2, Connection connShots1, Connection connShots2, boolean onlyCurrentSeason, String currentSeasonType) throws InterruptedException {
        HashSet<String> allExistingTables = findExistingTables(connShots1, ScraperUtilsInterface.super.getSchemaName(schemaShots1Alias));
        while (true) {
            //Exits while loop when queue return null
            Player polledPlayer = RunHandler.pollQueue();
            if (polledPlayer == null) {
                break;
            } else {
                int playerID = polledPlayer.getPlayerId();
                String lastNameOrig = polledPlayer.getLastName();
                String firstNameOrig = polledPlayer.getFirstName();
                String lastName = lastNameOrig.replaceAll("[^A-Za-z0-9]", "");
                String firstName = firstNameOrig.replaceAll("[^A-Za-z0-9]", "");
                //Gathers all active years for polled player
                ResultSet playerActivityResultSet = findPlayerActivity(lastName, firstName, playerID, onlyCurrentSeason, currentSeasonType, connPlayers1);
                String year, playerTableName;
                ArrayList<String> seasonTypes;
                try {
                    //Iterate through ResultSet
                    //If onlyCurrentSeason is true and player is not active in the current season type, ResultSet will be empty (next() will end loop)
                    while (playerActivityResultSet != null && playerActivityResultSet.next()) {
                        //Current year of the iteration
                        year = playerActivityResultSet.getString("year");
                        seasonTypes = new ArrayList<>();
                        //Update list of active season types for current year to scrape
                        //Add only the current season type if only scraping new shots (player is guaranteed to be active in season type)
                        if (onlyCurrentSeason) {
                            seasonTypes.add(mapDBColumnToSeasonType.get(currentSeasonType));
                        } else {
                            //Check each season type for activity
                            //If active, add to list of seasons to search
                            for (String key : mapDBColumnToSeasonType.keySet()) {
                                if (playerActivityResultSet.getInt(key) == 1) {
                                    seasonTypes.add(mapDBColumnToSeasonType.get(key));
                                }
                            }
                        }
                        //For each active season type for the given year
                        for (String eachSeasonType : seasonTypes) {
                            //Format table name
                            playerTableName = lastName + "_" + firstName + "_" + playerID + "_" + year.substring(0, 4) + "_" + year.substring(5) + "_" + eachSeasonType.replace(" ", "");
                            //Check if table exists already in database
                            //If scraping all tables, skip if table exists already
                            if (onlyCurrentSeason || !allExistingTables.contains(playerTableName)) {
                                //URL parameters can be slightly different from normal
                                //Get the shot data for the current parameters
                                JSONArray allShotsAsJSONArray = searchForShots(year, playerID, mapDBColumnToURLParamName.get(eachSeasonType));
                                createIndividualSeasonTable(playerTableName, connShots1, connShots2);
                                //If there is at least 1 shot recorded that player during that season
                                if (allShotsAsJSONArray != null && !allShotsAsJSONArray.isEmpty()) {
                                    HashSet<String> existingUniqueShotIds = new HashSet<>();
                                    if (onlyCurrentSeason) {
                                        findExistingShots(connShots1, playerTableName, existingUniqueShotIds);
                                    }
                                    insertShots(playerTableName, firstNameOrig, lastNameOrig, year, eachSeasonType, allShotsAsJSONArray, existingUniqueShotIds,
                                            connShots1, connShots2);
                                } else {
                                    LOGGER.info("\nTABLE NAME: " + playerTableName + "\n                                    NO SHOTS TAKEN");
                                }
                            }
                        }
                    }
                    playerActivityResultSet.close();
                } catch (Exception ex) {
                    LOGGER.error(ex.getMessage());
                }
            }
            //Pause to prevent server denying request
            if (RunHandler.peekQueue() != null) {
                Thread.sleep((long) (Math.random() * 10000));
            }
        }
        RunHandler.addToNewShotCount(totalNewShotsAdded);
    }

    /**
     * Generates bulk of SQL INSERT statement for a given table name
     *
     * @param tableName table name to receive data
     * @return String of SQL
     */
    private String createShotInsertSQL(String tableName) {
        return "INSERT INTO " + tableName
                + "(uniqueshotid,playerid,playerlast,playerfirst,season,"
                + "seasontype,gameid,gameeventid,calendar,clock,"
                + "minutes,seconds,x,y,distance,"
                + "make,period,shottype,playtype,teamid,"
                + "teamname,awayteamid,awayteamname,hometeamid,hometeamname,"
                + "athome,shotzonebasic,shotzonearea,shotzonerange )"
                + "VALUES(?,?,?,?,?"
                + ",?,?,?,?,?"
                + ",?,?,?,?,?"
                + ",?,?,?,?,?"
                + ",?,?,?,?,?"
                + ",?,?,?,?)";
    }

    /**
     * Organizes shot data for database entry
     *
     * @param playerTableName       player table name
     * @param firstNameOrig         player's real first name
     * @param lastNameOrig          player's real last name
     * @param year                  year
     * @param seasonType            season type
     * @param allShotsAsJSONArray   array of all shots scraped for the current parameters
     * @param existingUniqueShotIDs set of all shots already in database
     * @param connShots1            connection to first shot database
     * @param connShots2            connection to second shot database
     */
    protected void insertShots(String playerTableName, String firstNameOrig, String lastNameOrig, String year, String seasonType, JSONArray allShotsAsJSONArray, HashSet<String> existingUniqueShotIDs,
                               Connection connShots1, Connection connShots2) {
        try {
            ArrayList<PreparedStatement> allPreparedStatements = new ArrayList<>();
            allPreparedStatements.add(connShots1.prepareStatement(createShotInsertSQL(playerTableName)));
            allPreparedStatements.add(connShots1.prepareStatement(createShotInsertSQL("all_shots")));
            if (connShots1 != connShots2) {
                allPreparedStatements.add(connShots2.prepareStatement(createShotInsertSQL(playerTableName)));
                allPreparedStatements.add(connShots2.prepareStatement(createShotInsertSQL("all_shots")));
            }
            HashSet<String> newUniqueIds = new HashSet<>();
            //Iterate through all gathered shot data and filter out shots already in database
            JSONArray eachShotJSONArray;
            for (int index = 0; index < allShotsAsJSONArray.length(); index++) {
                eachShotJSONArray = allShotsAsJSONArray.getJSONArray(index);
                String uniqueID = eachShotJSONArray.getInt(3) + "-" + eachShotJSONArray.getInt(1) + "-" + eachShotJSONArray.getInt(2);
                if (!existingUniqueShotIDs.contains(uniqueID)) {
                    for (int i = 0; i < eachShotJSONArray.length(); i++) {
                        try {
                            //Format shot data for prepared statement
                            //Some values are recorded as integers, some as strings
                            switch (i) {
                                //UniqueID instead of default value from source
                                case 0:
                                    insertParametersIntoAllPreparedStatements(allPreparedStatements, "string", 1, uniqueID);
                                    break;
                                //Normal strings
                                case 1:
                                case 6:
                                case 11:
                                case 12:
                                case 13:
                                case 14:
                                case 15:
                                    insertParametersIntoAllPreparedStatements(allPreparedStatements, "string", MAP_JSON_ARRAY_INDEX_TO_PS_INDEX.get(i), eachShotJSONArray.getString(i));
                                    break;
                                //Normal Integers
                                case 2:
                                case 3:
                                case 5:
                                case 7:
                                case 8:
                                case 9:
                                case 16:
                                case 17:
                                case 18:
                                    insertParametersIntoAllPreparedStatements(allPreparedStatements, "integer", MAP_JSON_ARRAY_INDEX_TO_PS_INDEX.get(i), eachShotJSONArray.getInt(i) + "");
                                    break;
                                //Makes
                                case 10:
                                    insertParametersIntoAllPreparedStatements(allPreparedStatements, "integer", MAP_JSON_ARRAY_INDEX_TO_PS_INDEX.get(i), eachShotJSONArray.getString(i).contains("Made") ? "1" : "0");
                                    break;
                                //Dates
                                case 21:
                                    insertParametersIntoAllPreparedStatements(allPreparedStatements, "date", MAP_JSON_ARRAY_INDEX_TO_PS_INDEX.get(i), eachShotJSONArray.getString(i));
                                    break;
                                //Home and away team names
                                case 22:
                                case 23:
                                    insertParametersIntoAllPreparedStatements(allPreparedStatements, "string", MAP_JSON_ARRAY_INDEX_TO_PS_INDEX.get(i), eachShotJSONArray.getString(i));
                                    int homeID = -1;
                                    //Team IDs
                                    //If team abbreviation is a special abbreviation
                                    if (specialTeams.containsKey(eachShotJSONArray.getString(i))) {
                                        homeID = this.teamAbbrMap.get(specialTeams.get(eachShotJSONArray.getString(i)));
                                        insertParametersIntoAllPreparedStatements(allPreparedStatements, "integer", MAP_JSON_ARRAY_INDEX_TO_PS_INDEX.get(i) - 1, this.teamAbbrMap.get(specialTeams.get(eachShotJSONArray.getString(i))) + "");
                                    } else if (this.teamAbbrMap.containsKey(eachShotJSONArray.getString(i))) {
                                        //Normal team abbreviation
                                        homeID = this.teamAbbrMap.get(eachShotJSONArray.getString(i));
                                        insertParametersIntoAllPreparedStatements(allPreparedStatements, "integer", MAP_JSON_ARRAY_INDEX_TO_PS_INDEX.get(i) - 1, this.teamAbbrMap.get(eachShotJSONArray.getString(i)) + "");
                                    } else {
                                        //If unknown or missing abbreviation, insert -1
                                        insertParametersIntoAllPreparedStatements(allPreparedStatements, "integer", MAP_JSON_ARRAY_INDEX_TO_PS_INDEX.get(i) - 1, "-1");
                                    }
                                    //At home (1=true, 0=false)
                                    if (i == 22) {
                                        insertParametersIntoAllPreparedStatements(allPreparedStatements, "integer", 26, (eachShotJSONArray.getInt(5) == homeID) ? "1" : "0");
                                    }
                                    break;
                            }
                        } catch (Exception ex) {
                            LOGGER.error(ex.getMessage());
                        }
                    }
                    try {
                        //Last name
                        insertParametersIntoAllPreparedStatements(allPreparedStatements, "string", 3, lastNameOrig);
                        //First name
                        insertParametersIntoAllPreparedStatements(allPreparedStatements, "string", 4, firstNameOrig);
                        //Year
                        insertParametersIntoAllPreparedStatements(allPreparedStatements, "string", 5, year);
                        //Season Type
                        insertParametersIntoAllPreparedStatements(allPreparedStatements, "string", 6, seasonType);
                        //Time
                        String secondsFormat = eachShotJSONArray.getInt(9) < 10 ? "0" + eachShotJSONArray.getInt(9) : eachShotJSONArray.getInt(9) + "";
                        insertParametersIntoAllPreparedStatements(allPreparedStatements, "time", 10, String.format("%d:", eachShotJSONArray.getInt(8)) + secondsFormat);
                    } catch (Exception ex) {
                        LOGGER.error(ex.getMessage());
                    }
                    //Execute PreparedStatements
                    if (!newUniqueIds.contains(uniqueID)) {
                        newUniqueIds.add(uniqueID);
                        for (PreparedStatement stmt : allPreparedStatements) {
                            try {
                                stmt.execute();
                            } catch (SQLException ex) {
                                LOGGER.error(ex.getMessage());
                            }
                        }
                    }
                }
            }
            LOGGER.info("\nTABLE NAME: " + playerTableName + "\n          TOTAL SHOTS: " + (newUniqueIds.size() + existingUniqueShotIDs.size()) + "\n" + "          NEW SHOTS ADDED: " + newUniqueIds.size());
            totalNewShotsAdded += newUniqueIds.size();
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    /**
     * Inserts parameters of various types into prepared statements
     *
     * @param statements     list of all prepared statements
     * @param dataType       data type of value, as a String
     * @param parameterIndex index of prepared statement where value should be inserted
     * @param value          the data to be inserted into prepared statement
     * @throws SQLException   If setting prepared statement fails
     * @throws ParseException If parsing date or time fails
     */
    protected void insertParametersIntoAllPreparedStatements(ArrayList<PreparedStatement> statements, String dataType, int parameterIndex, String value) throws SQLException, ParseException {
        DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
        dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        DateFormat timeFormatter = new SimpleDateFormat("mm:ss");
        timeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        for (PreparedStatement statement : statements) {
            switch (dataType) {
                case "string":
                    statement.setString(parameterIndex, value);
                    break;
                case "integer":
                    statement.setInt(parameterIndex, Integer.parseInt(value));
                    break;
                case "time":
                    statement.setTime(parameterIndex, new java.sql.Time(timeFormatter.parse(value).getTime()), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                    break;
                case "date":
                    statement.setDate(parameterIndex, new java.sql.Date(dateFormatter.parse(value).getTime()), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                    break;
                default:
                    throw new NoSuchElementException("Invalid data type parameter");
            }
        }
    }

    /**
     * Fetches URL and get shot data
     *
     * @param year   year
     * @param id     player ID
     * @param season season type
     * @return list of shots gathered from URL, as JSONArrays, or null if the response is not received
     */
    private JSONArray searchForShots(String year, int id, String season) {
        String url = "https://stats.nba.com/stats/shotchartdetail?AheadBehind=&CFID=33&CFPARAMS="
                + year + "&ClutchTime=&Conference=&ContextFilter=&ContextMeasure=FGA&DateFrom=&DateTo=&Division=&EndPeriod=10&EndRange=28800&GROUP_ID=&GameEventID=&GameID=&GameSegment=&GroupID=&GroupMode=&GroupQuantity=5&LastNGames=0&LeagueID=00&Location=&Month=0&OnOff=&OpponentTeamID=0&Outcome=&PORound=0&Period=0&PlayerID="
                + id + "&PlayerID1=&PlayerID2=&PlayerID3=&PlayerID4=&PlayerID5=&PlayerPosition=&PointDiff=&Position=&RangeType=0&RookieYear=&Season=&SeasonSegment=&SeasonType="
                + season + "&ShotClockRange=&StartPeriod=1&StartRange=0&StarterBench=&TeamID=0&VsConference=&VsDivision=&VsPlayerID1=&VsPlayerID2=&VsPlayerID3=&VsPlayerID4=&VsPlayerID5=&VsTeamID=";
        int exceptionRetryCounter = 0;
        while (exceptionRetryCounter < 3) {
            try {
                String response = ScraperUtilsInterface.super.fetchSpecificURL(url);
                LOGGER.debug("Response from " + url + ": " + response);
                return new JSONObject(response).getJSONArray("resultSets").getJSONObject(0).getJSONArray("rowSet");
            } catch (Exception ex) {
                LOGGER.error(ex.getMessage());
                exceptionRetryCounter++;
            }
        }
        return null;
    }

    /**
     * Creates large shot table with column indexing
     *
     * @param connShots1 connection to first shot database
     * @param connShots2 connection to second shot database
     * @throws SQLException If creating table fails
     */
    protected void createAllShotsTable(Connection connShots1, Connection connShots2) throws SQLException {
        String createAllShotTable = "CREATE TABLE IF NOT EXISTS all_shots (\n" +
                "`uniqueshotid` varchar(100) NOT NULL,\n" +
                "  `playerid` int NOT NULL,\n" +
                "  `playerlast` varchar(45) NOT NULL,\n" +
                "  `playerfirst` varchar(45) DEFAULT NULL,\n" +
                "  `season` varchar(10) NOT NULL,\n" +
                "  `seasontype` varchar(20) NOT NULL,\n" +
                "  `gameid` int NOT NULL,\n" +
                "  `gameeventid` int NOT NULL,\n" +
                "  `calendar` date NOT NULL,\n" +
                "  `clock` time NOT NULL,\n" +
                "  `minutes` int NOT NULL,\n" +
                "  `seconds` int NOT NULL,\n" +
                "  `x` int NOT NULL,\n" +
                "  `y` int NOT NULL,\n" +
                "  `distance` int NOT NULL,\n" +
                "  `make` tinyint NOT NULL,\n" +
                "  `period` int NOT NULL,\n" +
                "  `shottype` varchar(20) NOT NULL,\n" +
                "  `playtype` varchar(45) NOT NULL,\n" +
                "  `teamid` int NOT NULL,\n" +
                "  `teamname` varchar(40) NOT NULL,\n" +
                "  `awayteamid` int NOT NULL,\n" +
                "  `awayteamname` varchar(40) NOT NULL,\n" +
                "  `hometeamid` int NOT NULL,\n" +
                "  `hometeamname` varchar(40) NOT NULL,\n" +
                "  `athome` tinyint NOT NULL,\n" +
                "  `shotzonebasic` varchar(25) NOT NULL,\n" +
                "  `shotzonearea` varchar(25) NOT NULL,\n" +
                "  `shotzonerange` varchar(25) NOT NULL,\n" +
                "  PRIMARY KEY (`uniqueshotid`),\n" +
                "  UNIQUE KEY `all_shots_UN` (`uniqueshotid`),\n" +
                "  KEY `index_playerid` (`playerid`)"
                + ")\n"
                + "ENGINE=InnoDB\n"
                + "DEFAULT CHARSET=utf8mb4\n"
                + "COLLATE=utf8mb4_0900_ai_ci";
        connShots1.prepareStatement(createAllShotTable).execute();
        connShots2.prepareStatement(createAllShotTable).execute();
    }

    /**
     * Creates a shot table for a single player
     *
     * @param playerTableName table name
     * @param connShots1      connection to first shot database
     * @param connShots2      connection to second shot database
     * @throws SQLException If creating table fails
     */
    protected void createIndividualSeasonTable(String playerTableName, Connection connShots1, Connection connShots2) throws SQLException {
        String createTable = "CREATE TABLE IF NOT EXISTS " + playerTableName + " (\n"
                + "	uniqueshotid varchar(100) NOT NULL,\n"
                + "	playerid INT NOT NULL,\n"
                + "	playerlast varchar(45) NOT NULL,\n"
                + "	playerfirst varchar(45) NULL,\n"
                + "	season varchar(10) NOT NULL,\n"
                + "	seasontype varchar(20) NOT NULL,\n"
                + "	gameid INT NOT NULL,\n"
                + "	gameeventid INT NOT NULL,\n"
                + "	calendar DATE NOT NULL,\n"
                + "	clock TIME NOT NULL ,\n"
                + "	minutes INT NOT NULL,\n"
                + "	seconds INT NOT NULL,\n"
                + "	x INT NOT NULL,\n"
                + "	y INT NOT NULL,\n"
                + "	distance INT NOT NULL,\n"
                + "	make TINYINT NOT NULL,\n"
                + "	period INT NOT NULL,\n"
                + "	shottype varchar(20) NOT NULL,\n"
                + "	playtype varchar(45) NOT NULL,\n"
                + "	teamid INT NOT NULL,\n"
                + "	teamname varchar(40) NOT NULL,\n"
                + "	awayteamid INT NOT NULL,\n"
                + "	awayteamname varchar(40) NOT NULL,\n"
                + "	hometeamid INT NOT NULL,\n"
                + "	hometeamname varchar(40) NOT NULL,\n"
                + "	athome TINYINT NOT NULL,\n"
                + "	shotzonebasic varchar(25) NOT NULL,\n"
                + "	shotzonearea varchar(25) NOT NULL,\n"
                + "	shotzonerange varchar(25) NOT NULL,\n"
                + "	CONSTRAINT " + playerTableName + "_UN UNIQUE KEY (uniqueshotid),\n"
                + "	CONSTRAINT " + playerTableName + "_PK PRIMARY KEY (uniqueshotid)\n"
                + ")\n"
                + "ENGINE=InnoDB\n"
                + "DEFAULT CHARSET=utf8mb4\n"
                + "COLLATE=utf8mb4_0900_ai_ci";
        connShots1.prepareStatement(createTable).execute();
        connShots2.prepareStatement(createTable).execute();
    }
}