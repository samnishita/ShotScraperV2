package com.example.ShotScraperV2;

import org.openqa.selenium.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpTimeoutException;
import java.sql.*;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;

public class IndividualPlayerScraper {
    private Logger LOGGER = LoggerFactory.getLogger(IndividualPlayerScraper.class);
    private Connection connPlayers1 = null, connPlayers2 = null;
    private String schema1, schema2, location1, location2;
    private ArrayList<String> regSeasons, postSeasons, preSeasons;

    private ScraperUtilityFunctions scraperUtilityFunctions = new ScraperUtilityFunctions();

    public IndividualPlayerScraper(String schema1, String location1, String schema2, String location2) {
        try {
            connPlayers1 = scraperUtilityFunctions.setNewConnection(schema1, location1);
            connPlayers2 = scraperUtilityFunctions.setNewConnection(schema2, location2);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.schema1 = schema1;
        this.schema2 = schema2;
        this.location1 = location1;
        this.location2 = location2;
        LOGGER.info("Initialized PlayerInfoFinder for " + location1 + " and " + location2);
    }

    public void getAllActiveYearsUsingMain(boolean reRun) {
        ArrayList<String> excludedIDs = new ArrayList(Arrays.asList("41", "986", "202070", "201195", "202221", "201998", "201987",
                "202364", "202067", "202238", "1626122", "202358", "202392", "1629129", "202375"));
        //These players may make a comeback
        ArrayList<String> tempSkipIds = new ArrayList(Arrays.asList("1630258", "1629341", "1629624", "1630209", "1630492"));
        while (true) {
            HashMap<String, String> eachPlayerHashMap = RunHandler.popConcurrentHashMap();
            String playerTableName;
            if (eachPlayerHashMap == null) {
                break;
            } else {
                String eachID = eachPlayerHashMap.get("playerID");
                String lastNameOrig = eachPlayerHashMap.get("lastNameOrig");
                String firstNameOrig = eachPlayerHashMap.get("firstNameOrig");
                String lastName = lastNameOrig.replaceAll("[^A-Za-z0-9]", "");
                String firstName = firstNameOrig.replaceAll("[^A-Za-z0-9]", "");
                if (excludedIDs.contains(eachID) || tempSkipIds.contains(eachID) || (eachPlayerHashMap.get("firstactiveyear").equals("2021-22") && eachPlayerHashMap.get("firstactiveyear").equals(eachPlayerHashMap.get("mostrecentactiveyear")))) {
                    continue;
                }
                playerTableName = lastName + "_" + firstName + "_" + eachID + "_individual_data";
                int existingTableCount = 0;
                try {
                    ResultSet rsTables = connPlayers1.getMetaData().getTables(this.schema1, null, playerTableName,
                            new String[]{"TABLE"});
                    while (rsTables.next()) {
                        existingTableCount++;
                    }
                    int shouldReRun = reRun ? 1 : 0;
                    if (existingTableCount <= shouldReRun) {
                        try {
                            loopSearchIfError(eachID, firstName, lastName);
                            HashSet<String> allYears = new HashSet();
                            this.regSeasons.stream().filter(eachSeason -> Integer.parseInt(eachSeason.substring(0, 4)) >= 1996).forEach(eachSeason -> allYears.add(eachSeason));
                            this.postSeasons.stream().filter(eachSeason -> Integer.parseInt(eachSeason.substring(0, 4)) >= 1996).forEach(eachSeason -> allYears.add(eachSeason));
                            this.preSeasons.stream().filter(eachSeason -> Integer.parseInt(eachSeason.substring(0, 4)) >= 2005).forEach(eachSeason -> allYears.add(eachSeason));
                            try {
                                if (allYears.size() > 0 && Integer.parseInt(Collections.max(allYears).substring(0, 4)) >= 1996) {
                                    createIndividualDataTable(playerTableName);
                                    groupYears(allYears, playerTableName, reRun);
                                } else {
                                    LOGGER.info("NO YEARS FOR " + firstName + " " + lastName + " (" + eachID + ")");
                                }
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        } catch (TimeoutException ex) {

                        }
                    }
                } catch (Exception ex) {

                }
            }
        }
    }

    protected void searchForPlayer(String id) throws HttpTimeoutException {
        try {
            this.regSeasons = new ArrayList<>();
            this.postSeasons = new ArrayList<>();
            this.preSeasons = new ArrayList<>();
            String response = scraperUtilityFunctions.fetchSpecificURL("https://stats.nba.com/stats/playerprofilev2?LeagueID=00&PerMode=PerGame&PlayerID=" + id);
            StringBuilder sb = new StringBuilder();
            sb.append(id).append(": ");
            JSONObject responseJSON = new JSONObject(response);
            try {
                sb = recordSeasons(responseJSON, sb, "SeasonTotalsRegularSeason", this.regSeasons, "Regular Seasons", 0);
                sb = recordSeasons(responseJSON, sb, "SeasonTotalsPostSeason", this.postSeasons, "PostSeasons", 2);
                sb = recordSeasons(responseJSON, sb, "SeasonTotalsPreseason", this.preSeasons, "PreSeasons", 8);
            } catch (Exception ex) {
            }
            LOGGER.info(sb.toString());
        } catch (HttpTimeoutException ex) {
            throw new HttpTimeoutException("");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private StringBuilder recordSeasons(JSONObject text, StringBuilder sb, String desiredString, ArrayList<String> list, String sbString, int eachIndex) {
        String seasonType = text.getJSONArray("resultSets").getJSONObject(eachIndex).getString("name");
        String year;
        JSONArray rowSets;
        int firstYear = desiredString == "SeasonTotalsPreseason" ? 2005 : 1996;
        if (seasonType.equals(desiredString)) {
            rowSets = text.getJSONArray("resultSets").getJSONObject(eachIndex).getJSONArray("rowSet");
            if (rowSets.length() != 0) {
                for (int index = 0; index < rowSets.length(); index++) {
                    year = rowSets.getJSONArray(index).getString(1);
                    if (!list.contains(year) && Integer.parseInt(year.substring(0, 4)) >= firstYear) {
                        list.add(year);
                    }
                }
            }
        }
        if (!sb.toString().contains(sbString)) {
            sb.append("(" + sbString + " : ").append(list.size()).append(") ");
        }
        return sb;
    }

    public String generatePlayerSelectStatement(char startCharInclusive, char endCharInclusive, boolean onlyActivePlayers) throws Exception {
        String alp = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        int startCharIndex = alp.indexOf((startCharInclusive + "").toUpperCase());
        int endCharIndex = alp.indexOf((endCharInclusive + "").toUpperCase());
        if (endCharIndex < startCharIndex || startCharIndex < 0 || startCharIndex > 25 || endCharIndex < 0 || endCharIndex > 25) {
            throw new IllegalArgumentException("Invalid selection.");
        }
        StringBuilder sbSelect = new StringBuilder(alp.charAt(startCharIndex) + "");
        for (int i = startCharIndex + 1; i <= endCharIndex; i++) {
            sbSelect.append("|").append(alp.charAt(i));
        }
        StringBuilder sqlSelect = new StringBuilder("SELECT * FROM player_relevant_data WHERE lastname REGEXP '^(" + sbSelect.toString() + ")'");
        if (onlyActivePlayers) {
            sqlSelect.append(" AND currentlyactive=1");
        }
        return sqlSelect.toString();
    }

    protected void insertGivenYear(String playerTableName, String year) {
        try {
            String sqlInsert = "INSERT INTO " + playerTableName + "(year) VALUES (?)";
            PreparedStatement stmt = connPlayers1.prepareStatement(sqlInsert);
            stmt.setString(1, year);
            stmt.execute();
            stmt = connPlayers2.prepareStatement(sqlInsert);
            stmt.setString(1, year);
            stmt.execute();
            LOGGER.info("Inserting " + playerTableName + " : " + year);
        } catch (SQLException ex) {

        }
    }

    protected void createIndividualDataTable(String playerTableName) throws SQLException {
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

    protected void updateGivenSeasons(String playerTableName, String year, String seasonType) {
        try {
            String sqlUpdate = "UPDATE " + playerTableName + " SET " + seasonType + "= 1 WHERE year = \"" + year + "\"";
            connPlayers1.prepareStatement(sqlUpdate).execute();
            connPlayers2.prepareStatement(sqlUpdate).execute();
            LOGGER.info("Updating " + playerTableName + " : " + year + " " + seasonType + " -> 1");
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    public void updateForCurrentYear( boolean onlyActivePlayers, String seasonType) {
        while (true) {
            try {
                HashMap<String, String> eachPlayerHashMap = RunHandler.popConcurrentHashMap();
                if (eachPlayerHashMap == null) {
                    break;
                }
                String eachID = eachPlayerHashMap.get("playerID");
                String lastNameOrig = eachPlayerHashMap.get("lastNameOrig");
                String firstNameOrig = eachPlayerHashMap.get("firstNameOrig");
                String lastName = lastNameOrig.replaceAll("[^A-Za-z0-9]", "");
                String firstName = firstNameOrig.replaceAll("[^A-Za-z0-9]", "");
                String playerTableName = lastName + "_" + firstName + "_" + eachID + "_individual_data";
                //See if _individual_data table already exists
                int existingTableCount = 0;
                if ((Integer.parseInt(eachPlayerHashMap.get("currentlyactive")) == 1) == onlyActivePlayers) {
                    ResultSet rsTables = connPlayers1.getMetaData().getTables(this.schema1, null, playerTableName,
                            new String[]{"TABLE"});
                    while (rsTables.next()) {
                        existingTableCount++;
                    }
                }

                if (existingTableCount == 1 && (Integer.parseInt(eachPlayerHashMap.get("currentlyactive")) == 1) == onlyActivePlayers) {
                    ResultSet rsCurSeasActive = connPlayers1.prepareStatement("SELECT " + seasonType + " FROM " + playerTableName + " WHERE year = '" + scraperUtilityFunctions.getCurrentYear() + "'").executeQuery();
                    boolean shouldCont = true;
                    while (rsCurSeasActive.next()) {
                        if (rsCurSeasActive.getInt(seasonType) == 1) {
                            shouldCont = false;
                        }
                    }
                    if (shouldCont) {
                        try {
                            loopSearchIfError(eachID, firstName, lastName);
                            HashSet<String> allYears = new HashSet();
                            this.regSeasons.stream()
                                    .filter(eachSeason -> Integer.parseInt(eachSeason.substring(0, 4)) >= 1996)
                                    .forEach(eachSeason -> allYears.add(eachSeason));
                            this.postSeasons.stream()
                                    .filter(eachSeason -> Integer.parseInt(eachSeason.substring(0, 4)) >= 1996)
                                    .forEach(eachSeason -> allYears.add(eachSeason));
                            this.preSeasons.stream()
                                    .filter(eachSeason -> Integer.parseInt(eachSeason.substring(0, 4)) >= 2005)
                                    .forEach(eachSeason -> allYears.add(eachSeason));
                            groupYears(allYears, playerTableName, true);
                        } catch (TimeoutException ex) {

                        }
                    }
                }
            } catch (Exception ex) {

            }
        }
        LOGGER.info("End of updateForCurrentYear()");
    }

    private void groupYears(HashSet<String> allYears, String playerTableName, boolean reRun) throws SQLException {
        final String TABLE_NAME = playerTableName;
        HashSet<String> regularSeasonsInDB = new HashSet<>();
        HashSet<String> postSeasonsInDB = new HashSet<>();
        HashSet<String> preSeasonsInDB = new HashSet<>();
        HashSet<String> allSeasonsInDB = new HashSet<>();
        if (reRun) {
            ResultSet allYearData = connPlayers1.prepareStatement("SELECT * FROM " + playerTableName).executeQuery();
            while (allYearData.next()) {
                allSeasonsInDB.add(allYearData.getString("year"));
                if (allYearData.getInt("reg") == 1) {
                    regularSeasonsInDB.add(allYearData.getString("year"));
                }
                if (allYearData.getInt("playoffs") == 1) {
                    postSeasonsInDB.add(allYearData.getString("year"));
                }
                if (allYearData.getInt("preseason") == 1) {
                    preSeasonsInDB.add(allYearData.getString("year"));
                }
            }
        }
        allYears.stream().sorted()
                .filter(eachYear -> Integer.parseInt(eachYear.substring(0, 4)) >= 1996 && !allSeasonsInDB.contains(eachYear))
                .forEach(eachYear -> insertGivenYear(TABLE_NAME, eachYear));
        this.regSeasons.stream().sorted()
                .filter(eachYear -> Integer.parseInt(eachYear.substring(0, 4)) >= 1996 && !regularSeasonsInDB.contains(eachYear))
                .forEach(eachReg -> updateGivenSeasons(TABLE_NAME, eachReg, "reg"));
        this.postSeasons.stream().sorted()
                .filter(eachYear -> Integer.parseInt(eachYear.substring(0, 4)) >= 1996 && !postSeasonsInDB.contains(eachYear))
                .forEach(eachPost -> updateGivenSeasons(TABLE_NAME, eachPost, "playoffs"));
        this.preSeasons.stream().sorted()
                .filter(eachYear -> Integer.parseInt(eachYear.substring(0, 4)) >= 2005 && !preSeasonsInDB.contains(eachYear))
                .forEach(eachPre -> updateGivenSeasons(TABLE_NAME, eachPre, "preseason"));
    }

    private void loopSearchIfError(String eachID, String firstName, String lastName) throws TimeoutException {
        int exceptionRetryCounter = 0;
        while (exceptionRetryCounter < 3) {
            try {
                searchForPlayer(eachID);
                exceptionRetryCounter = 5;
            } catch (HttpTimeoutException | TimeoutException ex) {
                if (exceptionRetryCounter >= 3) {
                    LOGGER.error("Timeout caught in search for " + firstName + " " + lastName + ", Skipping");
                    throw new TimeoutException();
                } else {
                    exceptionRetryCounter++;
                    LOGGER.error("Timeout caught in search for " + firstName + " " + lastName + ", Retrying (" + exceptionRetryCounter + ")");
                }
            }
        }
    }
}
