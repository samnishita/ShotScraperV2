package com.example.ShotScraperV2;

import com.example.ShotScraperV2.nbaobjects.Player;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest
@DisplayName("IndividualPlayerScraper")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IndividualPlayerScraperTests {

    @Autowired
    private RunHandler runHandler;

    private IndividualPlayerScraper individualPlayerScraper = new IndividualPlayerScraper("playertest", "playertest");

    /**
     * Clears test database before testing
     *
     * @throws SQLException If statement fails
     */
    @BeforeAll
    void dropAllTablesBefore() throws SQLException {
        Connection connPlayers = individualPlayerScraper.setNewConnection("playertest");
        ResultSet rsTables = connPlayers.prepareStatement("SHOW TABLES").executeQuery();
        while (rsTables.next()) {
            connPlayers.prepareStatement("DROP TABLE " + rsTables.getString(1)).execute();
        }
        rsTables.close();
        connPlayers.close();
    }

    /**
     * Tests that all years and activity statuses are found from sample data
     *
     * @throws IOException   If sample data file is not found
     * @throws JSONException If JSON parsing fails
     */
    @Test
    @DisplayName("should record all player active years and season types")
    void shouldRecordPlayerYears() throws IOException, JSONException {
        //Sample data location
        String response = Files.readString(Path.of("src/main/resources/TonyParkerScrapedDataSample.txt"), StandardCharsets.US_ASCII);
        JSONObject responseJSON = new JSONObject(response);
        HashMap<String, ArrayList<Integer>> yearSeasonActivityMap = new HashMap<>();
        individualPlayerScraper.recordSeasons(responseJSON, new StringBuilder(), yearSeasonActivityMap);
        assertEquals(createTonyParkerTestMap(), yearSeasonActivityMap);
    }

    /**
     * Tests that all years and activity statuses are saved for a player that doesn't yet exist in database
     *
     * @throws SQLException         If establishing connection or saving data to database fails
     * @throws InterruptedException If fetching data fails
     */
    @Test
    @DisplayName("gets all player years when player table doesn't exist in database")
    void shouldGetAllPlayerYearsForNewPlayer() throws SQLException, InterruptedException {
        //Add test player to queue
        Player tonyParker = new Player("2225", "Parker", "Tony", "0", "2001", "2018");
        runHandler.addToQueueForTesting(tonyParker);
        Connection connPlayers1 = individualPlayerScraper.setNewConnection("playertest");
        individualPlayerScraper.getPlayerActiveYears(connPlayers1, connPlayers1);
        //Query database for inserted player data
        HashMap<String, ArrayList<Integer>> yearSeasonActivityMap = new HashMap<>();
        ResultSet rs = connPlayers1.prepareStatement("SELECT * FROM parker_tony_2225_individual_data").executeQuery();
        while (rs.next()) {
            yearSeasonActivityMap.put(rs.getString("year"), new ArrayList<>(Arrays.asList(rs.getInt("reg"), rs.getInt("preseason"), rs.getInt("playoffs"))));
        }
        assertEquals(createTonyParkerTestMap(), yearSeasonActivityMap);
        rs.close();
        connPlayers1.close();
    }

    /**
     * Tests that all years and activity statuses are updated for a player that already exists in database
     *
     * @throws SQLException         If establishing connection or saving data to database fails
     * @throws InterruptedException If fetching data fails
     */
    @Test
    @DisplayName("gets all player years when player table is already partially filled")
    void shouldUpdatePlayerYearsForExistingPlayer() throws SQLException, InterruptedException {
        //Add test player to queue
        Player tonyParker = new Player("2225", "Parker", "Tony", "0", "2001", "2018");
        runHandler.addToQueueForTesting(tonyParker);
        Connection connPlayers1 = individualPlayerScraper.setNewConnection("playertest");
        //Create test player table and insert some incomplete data
        individualPlayerScraper.createIndividualDataTable("parker_tony_2225_individual_data", connPlayers1, connPlayers1);
        connPlayers1.prepareStatement("INSERT INTO parker_tony_2225_individual_data (year,reg,preseason,playoffs) VALUES ('2001-02',-1,-1,-1)," +
                "('2002-03',-1,-1,-1),('2003-04',-1,-1,-1),('2004-05',-1,-1,-1),('2005-06',-1,-1,-1),('2006-07',-1,-1,-1)").execute();
        individualPlayerScraper.getPlayerActiveYears(connPlayers1, connPlayers1);
        //Query database for inserted player data
        HashMap<String, ArrayList<Integer>> yearSeasonActivityMap = new HashMap<>();
        ResultSet rs = connPlayers1.prepareStatement("SELECT * FROM parker_tony_2225_individual_data").executeQuery();
        while (rs.next()) {
            yearSeasonActivityMap.put(rs.getString("year"), new ArrayList<>(Arrays.asList(rs.getInt("reg"), rs.getInt("preseason"), rs.getInt("playoffs"))));
        }
        assertEquals(createTonyParkerTestMap(), yearSeasonActivityMap);
        rs.close();
        connPlayers1.close();
    }

    /**
     * Creates map of known test player data that should exist in database
     *
     * @return hashmap with (K,V) of (year, array of [regular season activity status, preseason activity status, playoff activity status])
     */
    private HashMap<String, ArrayList<Integer>> createTonyParkerTestMap() {
        HashMap<String, ArrayList<Integer>> knownYearSeasonActivityMap = new HashMap<>();
        knownYearSeasonActivityMap.put("2001-02", new ArrayList<>(Arrays.asList(1, -1, 1)));
        knownYearSeasonActivityMap.put("2002-03", new ArrayList<>(Arrays.asList(1, -1, 1)));
        knownYearSeasonActivityMap.put("2003-04", new ArrayList<>(Arrays.asList(1, -1, 1)));
        knownYearSeasonActivityMap.put("2004-05", new ArrayList<>(Arrays.asList(1, -1, 1)));
        knownYearSeasonActivityMap.put("2005-06", new ArrayList<>(Arrays.asList(1, 1, 1)));
        knownYearSeasonActivityMap.put("2006-07", new ArrayList<>(Arrays.asList(1, 1, 1)));
        knownYearSeasonActivityMap.put("2007-08", new ArrayList<>(Arrays.asList(1, 1, 1)));
        knownYearSeasonActivityMap.put("2008-09", new ArrayList<>(Arrays.asList(1, 1, 1)));
        knownYearSeasonActivityMap.put("2009-10", new ArrayList<>(Arrays.asList(1, 1, 1)));
        knownYearSeasonActivityMap.put("2010-11", new ArrayList<>(Arrays.asList(1, 1, 1)));
        knownYearSeasonActivityMap.put("2011-12", new ArrayList<>(Arrays.asList(1, 1, 1)));
        knownYearSeasonActivityMap.put("2012-13", new ArrayList<>(Arrays.asList(1, 1, 1)));
        knownYearSeasonActivityMap.put("2013-14", new ArrayList<>(Arrays.asList(1, 1, 1)));
        knownYearSeasonActivityMap.put("2014-15", new ArrayList<>(Arrays.asList(1, 1, 1)));
        knownYearSeasonActivityMap.put("2015-16", new ArrayList<>(Arrays.asList(1, 1, 1)));
        knownYearSeasonActivityMap.put("2016-17", new ArrayList<>(Arrays.asList(1, 1, 1)));
        knownYearSeasonActivityMap.put("2017-18", new ArrayList<>(Arrays.asList(1, -1, 1)));
        knownYearSeasonActivityMap.put("2018-19", new ArrayList<>(Arrays.asList(1, 1, -1)));
        return knownYearSeasonActivityMap;
    }

    /**
     * Drops all tables in the test database after all tests are complete
     *
     * @throws SQLException If dropping tables fails
     */
    @AfterEach
    void dropAllTablesAfter() throws SQLException {
        Connection connPlayers = individualPlayerScraper.setNewConnection("playertest");
        ResultSet rsTables = connPlayers.prepareStatement("SHOW TABLES").executeQuery();
        while (rsTables.next()) {
            connPlayers.prepareStatement("DROP TABLE " + rsTables.getString(1)).execute();
        }
        rsTables.close();
        connPlayers.close();
    }
}
