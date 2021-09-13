package com.example.ShotScraperV2;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class ShotScraper implements ScraperUtilsInterface{
    private final Logger LOGGER = LoggerFactory.getLogger(ShotScraper.class);
    private String schemaShots1, locationShots1, schemaShots2, locationShots2, schemaPlayers1, locationPlayers1, schemaPlayers2, locationPlayers2;
    private Connection connShots1 = null, connPlayers1 = null, connShots2 = null, connPlayers2 = null;
    private ArrayList<String> seasonTypes = new ArrayList(Arrays.asList("Preseason", "Regular%20Season", "Playoffs"));
    private HashMap<String, Integer> teamAbbrMap;
    private HashMap<String, String> specialTeams;
    private int totalNewShotsAdded;
    private IndividualPlayerScraper individualPlayerScraper;

    public ShotScraper(String schemaShots1, String locationShots1, String schemaShots2, String locationShots2, String schemaPlayers1, String locationPlayers1, String schemaPlayers2, String locationPlayers2) {
        try {
            connShots1 = ScraperUtilsInterface.super.setNewConnection(schemaShots1, locationShots1);
            connPlayers1 = ScraperUtilsInterface.super.setNewConnection(schemaPlayers1, locationPlayers1);
            connShots2 = ScraperUtilsInterface.super.setNewConnection(schemaShots2, locationShots2);
            connPlayers2 = ScraperUtilsInterface.super.setNewConnection(schemaPlayers2, locationPlayers2);
            this.schemaShots1 = schemaShots1;
            this.locationShots1 = locationShots1;
            this.schemaShots2 = schemaShots2;
            this.locationShots2 = locationShots2;
            this.schemaPlayers1 = schemaPlayers1;
            this.locationPlayers1 = locationPlayers1;
            this.schemaPlayers2 = schemaPlayers2;
            this.locationPlayers2 = locationPlayers2;
            this.teamAbbrMap = new HashMap();
            ResultSet rsTeam = connPlayers1.prepareStatement("SELECT * FROM team_data").executeQuery();
            while (rsTeam.next()) {
                this.teamAbbrMap.put(rsTeam.getString("abbr"), rsTeam.getInt("id"));
            }
            createAllShotsTable();
        } catch (Exception ex) {

        }
        specialTeams = new HashMap();
        specialTeams.put("NJN", "BKN");
        specialTeams.put("VAN", "MEM");
        specialTeams.put("NOK", "NOP");
        specialTeams.put("NOH", "NOP");
        specialTeams.put("SEA", "OKC");
        specialTeams.put("CHH", "CHA");
        totalNewShotsAdded = 0;
        final ResourceBundle READER = ResourceBundle.getBundle("application");
        this.individualPlayerScraper = new IndividualPlayerScraper(READER.getString("playerschema1"),
                READER.getString("playerlocation1"), READER.getString("playerschema2"), READER.getString("playerlocation2"));
    }

    public void getEveryShotWithMainThread() {
        while (true) {
            HashMap<String, String> eachPlayerHashMap = RunHandler.popQueue();
            if (eachPlayerHashMap == null) {
                break;
            } else {
                int playerID = Integer.parseInt(eachPlayerHashMap.get("playerID"));
                String lastNameOrig = eachPlayerHashMap.get("lastNameOrig");
                String firstNameOrig = eachPlayerHashMap.get("firstNameOrig");
                String lastName = lastNameOrig.replaceAll("[^A-Za-z0-9]", "");
                String firstName = firstNameOrig.replaceAll("[^A-Za-z0-9]", "");
                ResultSet indivSet;
                try {
                    String indivSelect = "SELECT * FROM " + lastName + "_" + firstName + "_" + playerID + "_individual_data";
                    indivSet = connPlayers1.prepareStatement(indivSelect).executeQuery();
                } catch (SQLException ex) {
                    continue;
                }
                String year, seasonType;
                try {
                    //For each year the particular player is active
                    String playerTableName;
                    while (indivSet.next()) {
                        //Current year of the iteration
                        year = indivSet.getString("year");
                        //For each type of season (preseason, regular, and playoffs)
                        for (String eachSeasonType : this.seasonTypes) {
                            //If the iteration is preseason and either 1) the player wasn't active or 2) the year was before 2005-06
                            if (eachSeasonType.equals("Preseason") && (indivSet.getInt("preseason") == -1 || Integer.parseInt(year.substring(0, 4)) < 2005)) {
                                continue;
                            } //If the iteration is the regular season and the player wasn't active
                            else if (eachSeasonType.equals("Regular%20Season") && indivSet.getInt("reg") == -1) {
                                continue;
                            } //if the iteration is the playoffs and the player wasn't active
                            else if (eachSeasonType.equals("Playoffs") && indivSet.getInt("playoffs") == -1) {
                                continue;
                            }
                            //Fix special characters for certain situations
                            if (eachSeasonType.equals("Regular%20Season")) {
                                seasonType = "Regular Season";
                                playerTableName = lastName + "_" + firstName + "_" + playerID + "_" + year.substring(0, 4) + "_" + year.substring(5) + "_" + "RegularSeason";
                            } else {
                                seasonType = eachSeasonType;
                                playerTableName = lastName + "_" + firstName + "_" + playerID + "_" + year.substring(0, 4) + "_" + year.substring(5) + "_" + eachSeasonType;
                            }
                            //Check if table exists already in nbashots database
                            int tableCounter = 0;
                            ResultSet shotTablesRS = connShots1.getMetaData().getTables(this.schemaShots1, null, playerTableName, new String[]{"TABLE"});
                            while (shotTablesRS.next()) {
                                tableCounter++;
                            }
                            //If the table doesn't already exist
                            if (tableCounter == 0) {
                                //Get the shot data for the current parameters
                                createIndividualSeasonTable(playerTableName);
                                String urlSeasonType;
                                if (eachSeasonType.equals("Preseason")) {
                                    urlSeasonType = "Pre+Season";
                                } else if (eachSeasonType.equals("Regular%20Season")) {
                                    urlSeasonType = "Regular+Season";
                                } else {
                                    urlSeasonType = "Playoffs";
                                }

                                ArrayList<JSONArray> allShotsAsJSONArrays = searchForShots(year, playerID, urlSeasonType);
                                //Create the table (because it won't exist)
                                //If there is more than 1 shot recorded that player during that season
                                if (allShotsAsJSONArrays != null && !allShotsAsJSONArrays.isEmpty()) {
                                    //Insert values into recently generated table
                                    insertShots(playerTableName, firstNameOrig, lastNameOrig, year, seasonType, allShotsAsJSONArrays, new HashSet<String>());
                                } else {
                                    LOGGER.info("\nTABLE NAME: " + playerTableName + "\n                                    NO SHOTS TAKEN");
                                }
                            }
                            //If table already exists then nothing happens
                        }
                    }
                } catch (Exception ex) {

                }
            }
        }
        RunHandler.addToNewShotCount(totalNewShotsAdded);
    }

    private void insertShots(String playerTableName, String firstNameOrig, String lastNameOrig, String year, String seasonType, ArrayList<JSONArray> allShotsAsJSONArrays, HashSet<String> existingUniqueShotIDs) {
        final String YEAR = year;
        final String SEASON_TYPE = seasonType;
        try {
            final String shotInsert = "INSERT INTO " + playerTableName
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
            final String allShotInsert = "INSERT INTO all_shots"
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
            //Prepare SQL statement for insertion
            //For each shot
            DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
            dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            DateFormat timeFormatter = new SimpleDateFormat("mm:ss");
            timeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            final String FIRST_NAME_ORIG = firstNameOrig;
            final String LAST_NAME_ORIG = lastNameOrig;
            PreparedStatement stmtShot = connShots1.prepareStatement(shotInsert);
            PreparedStatement stmtShotLocal = connShots2.prepareStatement(shotInsert);
            PreparedStatement stmtAllShot = connShots1.prepareStatement(allShotInsert);
            PreparedStatement stmtAllShotLocal = connShots2.prepareStatement(allShotInsert);
            HashMap<Integer, Integer> mapJSONArrayIndexToPreparedStatementIndex = new HashMap<>();
            mapJSONArrayIndexToPreparedStatementIndex.put(1, 7);//gameID
            mapJSONArrayIndexToPreparedStatementIndex.put(2, 8);//gameEventID
            mapJSONArrayIndexToPreparedStatementIndex.put(3, 2);//playerID
            mapJSONArrayIndexToPreparedStatementIndex.put(5, 20);//teamID
            mapJSONArrayIndexToPreparedStatementIndex.put(6, 21);//teamName
            mapJSONArrayIndexToPreparedStatementIndex.put(7, 17);//period
            mapJSONArrayIndexToPreparedStatementIndex.put(8, 11);//minutes
            mapJSONArrayIndexToPreparedStatementIndex.put(9, 12);//seconds
            mapJSONArrayIndexToPreparedStatementIndex.put(10, 16);//make
            mapJSONArrayIndexToPreparedStatementIndex.put(11, 19);//playType
            mapJSONArrayIndexToPreparedStatementIndex.put(12, 18);//shotType
            mapJSONArrayIndexToPreparedStatementIndex.put(13, 27);//shotZoneBasic
            mapJSONArrayIndexToPreparedStatementIndex.put(14, 28);//shotZoneArea
            mapJSONArrayIndexToPreparedStatementIndex.put(15, 29);//shotZoneRange
            mapJSONArrayIndexToPreparedStatementIndex.put(16, 15);//distance
            mapJSONArrayIndexToPreparedStatementIndex.put(17, 13);//x
            mapJSONArrayIndexToPreparedStatementIndex.put(18, 14);//y
            mapJSONArrayIndexToPreparedStatementIndex.put(21, 9);//calendar
            mapJSONArrayIndexToPreparedStatementIndex.put(22, 25);//homeTeamName
            mapJSONArrayIndexToPreparedStatementIndex.put(23, 23);//awayTeamName
            HashSet<String> newUniqueIds = new HashSet<>();
            try {
                for (JSONArray eachShotJSONArray : allShotsAsJSONArrays) {
                    //allShotsAsJSONArrays.stream().forEach(eachShotJSONArray -> {
                    String uniqueID = eachShotJSONArray.getInt(3) + "-" + eachShotJSONArray.getInt(1) + "-" + eachShotJSONArray.getInt(2);
                    if (!existingUniqueShotIDs.contains(uniqueID)) {
                        Date date;
                        java.sql.Date sqlDate;
                        java.sql.Time sqlTime;
                        for (int i = 0; i < 29; i++) {
                            try {
                                //Some values are recorded as integers, some as strings
                                switch (i) {
                                    case 0:
                                        stmtAllShot.setString(1, uniqueID);
                                        stmtAllShotLocal.setString(1, uniqueID);
                                        stmtShot.setString(1, uniqueID);
                                        stmtShotLocal.setString(1, uniqueID);
                                        break;
                                    case 1:
                                    case 6:
                                    case 11:
                                    case 12:
                                    case 13:
                                    case 14:
                                    case 15:
                                        stmtAllShot.setString(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getString(i));
                                        stmtAllShotLocal.setString(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getString(i));
                                        stmtShot.setString(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getString(i));
                                        stmtShotLocal.setString(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getString(i));
                                        break;
                                    case 2:
                                    case 3:
                                    case 5:
                                    case 7:
                                    case 8:
                                    case 9:
                                    case 16:
                                    case 17:
                                    case 18:
                                        stmtAllShot.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getInt(i));
                                        stmtAllShotLocal.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getInt(i));
                                        stmtShot.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getInt(i));
                                        stmtShotLocal.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getInt(i));
                                        break;
                                    case 10:
                                        stmtAllShot.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getString(i).contains("Made") ? 1 : 0);
                                        stmtAllShotLocal.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getString(i).contains("Made") ? 1 : 0);
                                        stmtShot.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getString(i).contains("Made") ? 1 : 0);
                                        stmtShotLocal.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getString(i).contains("Made") ? 1 : 0);
                                        break;
                                    case 21:
                                        date = dateFormatter.parse(eachShotJSONArray.getString(i));
                                        sqlDate = new java.sql.Date(date.getTime());
                                        stmtAllShot.setDate(mapJSONArrayIndexToPreparedStatementIndex.get(i), sqlDate);
                                        stmtAllShotLocal.setDate(mapJSONArrayIndexToPreparedStatementIndex.get(i), sqlDate);
                                        stmtShot.setDate(mapJSONArrayIndexToPreparedStatementIndex.get(i), sqlDate);
                                        stmtShotLocal.setDate(mapJSONArrayIndexToPreparedStatementIndex.get(i), sqlDate);
                                        break;
                                    case 22:
                                    case 23:
                                        stmtAllShot.setString(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getString(i));
                                        stmtAllShotLocal.setString(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getString(i));
                                        stmtShot.setString(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getString(i));
                                        stmtShotLocal.setString(mapJSONArrayIndexToPreparedStatementIndex.get(i), eachShotJSONArray.getString(i));
                                        int homeID = -1;
                                        if (specialTeams.containsKey(eachShotJSONArray.getString(i))) {
                                            homeID = this.teamAbbrMap.get(specialTeams.get(eachShotJSONArray.getString(i)));
                                            stmtAllShot.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i) - 1, this.teamAbbrMap.get(specialTeams.get(eachShotJSONArray.getString(i))));
                                            stmtAllShotLocal.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i) - 1, this.teamAbbrMap.get(specialTeams.get(eachShotJSONArray.getString(i))));
                                            stmtShot.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i) - 1, this.teamAbbrMap.get(specialTeams.get(eachShotJSONArray.getString(i))));
                                            stmtShotLocal.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i) - 1, this.teamAbbrMap.get(specialTeams.get(eachShotJSONArray.getString(i))));
                                        } else if (this.teamAbbrMap.containsKey(eachShotJSONArray.getString(i))) {
                                            homeID = this.teamAbbrMap.get(eachShotJSONArray.getString(i));
                                            stmtAllShot.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i) - 1, this.teamAbbrMap.get(eachShotJSONArray.getString(i)));
                                            stmtAllShotLocal.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i) - 1, this.teamAbbrMap.get(eachShotJSONArray.getString(i)));
                                            stmtShot.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i) - 1, this.teamAbbrMap.get(eachShotJSONArray.getString(i)));
                                            stmtShotLocal.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i) - 1, this.teamAbbrMap.get(eachShotJSONArray.getString(i)));
                                        } else {
                                            stmtAllShot.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i) - 1, -1);
                                            stmtAllShotLocal.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i) - 1, -1);
                                            stmtShot.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i) - 1, -1);
                                            stmtShotLocal.setInt(mapJSONArrayIndexToPreparedStatementIndex.get(i) - 1, -1);
                                        }
                                        if (i == 22) {
                                            int atHome = (i == 22 && eachShotJSONArray.getInt(5) == homeID) ? 1 : 0;
                                            stmtAllShot.setInt(26, atHome);
                                            stmtAllShotLocal.setInt(26, atHome);
                                            stmtShot.setInt(26, atHome);
                                            stmtShotLocal.setInt(26, atHome);
                                        }
                                        break;
                                }
                            } catch (Exception ex) {
                                LOGGER.error(ex.getMessage());
                            }
                        }
                        try {
                            stmtAllShot.setString(3, LAST_NAME_ORIG);
                            stmtAllShotLocal.setString(3, LAST_NAME_ORIG);
                            stmtShot.setString(3, LAST_NAME_ORIG);
                            stmtShotLocal.setString(3, LAST_NAME_ORIG);

                            stmtAllShot.setString(4, FIRST_NAME_ORIG);
                            stmtAllShotLocal.setString(4, FIRST_NAME_ORIG);
                            stmtShot.setString(4, FIRST_NAME_ORIG);
                            stmtShotLocal.setString(4, FIRST_NAME_ORIG);

                            stmtAllShot.setString(5, YEAR);
                            stmtAllShotLocal.setString(5, YEAR);
                            stmtShot.setString(5, YEAR);
                            stmtShotLocal.setString(5, YEAR);

                            stmtAllShot.setString(6, SEASON_TYPE);
                            stmtAllShotLocal.setString(6, SEASON_TYPE);
                            stmtShot.setString(6, SEASON_TYPE);
                            stmtShotLocal.setString(6, SEASON_TYPE);

                            String secondsFormat = eachShotJSONArray.getInt(9) < 10 ? "0" + eachShotJSONArray.getInt(9) : eachShotJSONArray.getInt(9) + "";
                            date = timeFormatter.parse(String.format("%d:", eachShotJSONArray.getInt(8)) + secondsFormat);
                            sqlTime = new java.sql.Time(date.getTime());
                            stmtAllShot.setTime(10, sqlTime, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                            stmtAllShotLocal.setTime(10, sqlTime, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                            stmtShot.setTime(10, sqlTime, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                            stmtShotLocal.setTime(10, sqlTime, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                        } catch (Exception ex) {
                            LOGGER.error(ex.getMessage());
                        }
                        if (!newUniqueIds.contains(uniqueID)) {
                            newUniqueIds.add(uniqueID);
                            int errorTries1 = 0;
                            int errorTries2 = 2;
                            while (errorTries1 < 3 || errorTries2 < 3) {
                                if (errorTries1 < 3) {
                                    try {
                                        stmtAllShot.execute();
                                        errorTries1 = 5;
                                    } catch (SQLException ex) {
                                        LOGGER.error(ex.getMessage());
                                        errorTries1++;
                                    }
                                    try {
                                        stmtShot.execute();
                                    } catch (SQLException ex) {
                                        LOGGER.error(ex.getMessage());
                                    }
                                }
                                if (errorTries2 < 3) {
                                    try {
                                        stmtAllShotLocal.execute();
                                        stmtShotLocal.execute();
                                        errorTries2 = 5;
                                    } catch (Exception ex) {
                                        errorTries2++;
                                    }
                                }
                                if (errorTries1 >= 3 && errorTries1 != 5 && errorTries2 >= 3 && errorTries2 != 5) {
                                    throw new SQLException("Error inputting shots for " + playerTableName);
                                }
                            }
                        }
                    }
                }
                LOGGER.info("\nTABLE NAME: " + playerTableName + "\n          TOTAL SHOTS: " + (newUniqueIds.size() + existingUniqueShotIDs.size()) + "\n" + "          NEW SHOTS ADDED: " + newUniqueIds.size());
                totalNewShotsAdded = totalNewShotsAdded + newUniqueIds.size();
            } catch (SQLException ex) {
                LOGGER.error(ex.getMessage());
            }
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    private ArrayList<JSONArray> searchForShots(String year, int id, String season) {
        ArrayList<JSONArray> allShotsAsJSONArrays = new ArrayList<>();
        String url = "https://stats.nba.com/stats/shotchartdetail?AheadBehind=&CFID=33&CFPARAMS=" + year + "&ClutchTime=&Conference=&ContextFilter=&ContextMeasure=FGA&DateFrom=&DateTo=&Division=&EndPeriod=10&EndRange=28800&GROUP_ID=&GameEventID=&GameID=&GameSegment=&GroupID=&GroupMode=&GroupQuantity=5&LastNGames=0&LeagueID=00&Location=&Month=0&OnOff=&OpponentTeamID=0&Outcome=&PORound=0&Period=0&PlayerID=" + id + "&PlayerID1=&PlayerID2=&PlayerID3=&PlayerID4=&PlayerID5=&PlayerPosition=&PointDiff=&Position=&RangeType=0&RookieYear=&Season=&SeasonSegment=&SeasonType=" + season + "&ShotClockRange=&StartPeriod=1&StartRange=0&StarterBench=&TeamID=0&VsConference=&VsDivision=&VsPlayerID1=&VsPlayerID2=&VsPlayerID3=&VsPlayerID4=&VsPlayerID5=&VsTeamID=";
        try {
            String response = ScraperUtilsInterface.super.fetchSpecificURL(url);
            JSONArray rowSets = new JSONObject(response).getJSONArray("resultSets").getJSONObject(0).getJSONArray("rowSet");
            for (int index = 0; index < rowSets.length(); index++) {
                allShotsAsJSONArrays.add(rowSets.getJSONArray(index));
            }
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
        return allShotsAsJSONArrays;
    }

    protected void createAllShotsTable() throws SQLException {
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
                "  UNIQUE KEY `NewTable_UN` (`uniqueshotid`),\n" +
                "  KEY `index_playerid` (`playerid`)"
                + ")\n"
                + "ENGINE=InnoDB\n"
                + "DEFAULT CHARSET=utf8mb4\n"
                + "COLLATE=utf8mb4_0900_ai_ci";
        connShots1.prepareStatement(createAllShotTable).execute();
        connShots2.prepareStatement(createAllShotTable).execute();
    }

    protected void createIndividualSeasonTable(String playerTableName) throws SQLException {
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
                + "	CONSTRAINT NewTable_UN UNIQUE KEY (uniqueshotid),\n"
                + "	CONSTRAINT NewTable_PK PRIMARY KEY (uniqueshotid)\n"
                + ")\n"
                + "ENGINE=InnoDB\n"
                + "DEFAULT CHARSET=utf8mb4\n"
                + "COLLATE=utf8mb4_0900_ai_ci";
        connShots1.prepareStatement(createTable).execute();
        connShots2.prepareStatement(createTable).execute();
    }

    public void updateShotsForCurrentYear(String seasonTypeSelector) {
        while (true) {
            try {
                HashMap<String, String> eachPlayerHashMap = RunHandler.popQueue();
                if (eachPlayerHashMap == null) {
                    break;
                }
                int playerID = Integer.parseInt(eachPlayerHashMap.get("playerID"));
                String lastNameOrig = eachPlayerHashMap.get("lastNameOrig");
                String firstNameOrig = eachPlayerHashMap.get("firstNameOrig");
                String lastName = lastNameOrig.replaceAll("[^A-Za-z0-9]", "");
                String firstName = firstNameOrig.replaceAll("[^A-Za-z0-9]", "");
                switch (seasonTypeSelector) {
                    case "preseason":
                        processSingleSeasonType("preseason", "Preseason", "Preseason", "Preseason", lastName, lastNameOrig, firstName, firstNameOrig, playerID, ScraperUtilsInterface.super.getCurrentYear());
                        break;
                    case "regularseason":
                        processSingleSeasonType("reg", "RegularSeason", "Regular%20Season", "Regular Season", lastName, lastNameOrig, firstName, firstNameOrig, playerID, ScraperUtilsInterface.super.getCurrentYear());
                        break;
                    case "playoffs":
                        processSingleSeasonType("playoffs", "Playoffs", "Playoffs", "Playoffs", lastName, lastNameOrig, firstName, firstNameOrig, playerID, ScraperUtilsInterface.super.getCurrentYear());
                        break;
                    default:
                        throw new IllegalStateException("Invalid season type: " + seasonTypeSelector);
                }
                RunHandler.addToNewShotCount(totalNewShotsAdded);
            } catch (Exception ex) {
                LOGGER.error(ex.getMessage());
            }
        }
    }

    private void processSingleSeasonType(String seasonTypeQuery, String seasonTypeTableName, String seasonTypeURL, String seasonTypeDataEntry, String lastName, String lastNameOrig, String firstName, String firstNameOrig, int playerID, String currentYear) {
        try {
            String sqlGetSeasonActivity = "SELECT " + seasonTypeQuery + " FROM " + lastName + "_" + firstName + "_" + playerID + "_individual_data WHERE year = \"" + currentYear + "\"";
            ResultSet currentSeasonTypeActivityResultSet = connPlayers1.prepareStatement(sqlGetSeasonActivity).executeQuery();
            while (currentSeasonTypeActivityResultSet.next()) {
                if (currentSeasonTypeActivityResultSet.getInt("preseason") == 1) {
                    String tableName = lastName + "_" + firstName + "_" + playerID + "_" + currentYear.substring(0, 4) + "_" + currentYear.substring(5) + "_" + seasonTypeTableName;
                    createIndividualSeasonTable(tableName);
                    ResultSet existingShotsResultSet = connShots1.prepareStatement("SELECT uniqueshotid FROM " + tableName).executeQuery();
                    HashSet<String> existingUniqueIDs = new HashSet();
                    while (existingShotsResultSet.next()) {
                        existingUniqueIDs.add(existingShotsResultSet.getString("uniqueshotid"));
                    }
                    ArrayList<JSONArray> allShotsAsJSONArrays = searchForShots(currentYear, playerID, seasonTypeURL);
                    insertShots(tableName, firstNameOrig, lastNameOrig, currentYear, seasonTypeDataEntry, allShotsAsJSONArrays, existingUniqueIDs);
                }
            }
        } catch (Exception ex) {

        }
    }
}