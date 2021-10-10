package com.example.ShotScraperV2;

import com.example.ShotScraperV2.nbaobjects.Shot;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@DisplayName("ShotScraperTests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ShotScraperTests {
    @Autowired
    private AllTeamAndPlayerScraper allTeamAndPlayerScraper;
    private ShotScraper shotScraper = new ShotScraper("shottest", "shottest", "playertest", "playertest", new IndividualPlayerScraper("playertest", "playertest"));


    /**
     * Clears test databases before testing
     *
     * @throws SQLException If statement fails
     */
    @BeforeAll
    void dropAllTablesBefore() throws SQLException {
        IndividualPlayerScraper individualPlayerScraper = new IndividualPlayerScraper("playertest", "playertest");
        Connection connPlayers = individualPlayerScraper.setNewConnection("playertest");
        ResultSet rsTablesPlayers = connPlayers.prepareStatement("SHOW TABLES").executeQuery();
        while (rsTablesPlayers.next()) {
            connPlayers.prepareStatement("DROP TABLE " + rsTablesPlayers.getString(1)).execute();
        }
        rsTablesPlayers.close();
        connPlayers.close();
        Connection connShots = shotScraper.setNewConnection("shottest");
        ResultSet rsTablesShots = connShots.prepareStatement("SHOW TABLES").executeQuery();
        while (rsTablesShots.next()) {
            connShots.prepareStatement("DROP TABLE " + rsTablesShots.getString(1)).execute();
        }
        rsTablesShots.close();
        connShots.close();
    }

    /**
     * Creates a new ShotScraper object
     */
    @BeforeEach
    void createNewShotScraper() {
        shotScraper = new ShotScraper("shottest", "shottest", "playertest", "playertest", new IndividualPlayerScraper("playertest", "playertest"));
    }

    /**
     * Tests that Strings are correctly added to prepared statements
     *
     * @param testValue String to test
     * @throws SQLException   If prepared statement fails
     * @throws ParseException If parsing fails
     */
    @ParameterizedTest
    @DisplayName("inserts strings into prepared statement")
    @MethodSource("provideStringsForPreparedStatement")
    void shouldInsertStringIntoPreparedStatement(String testValue) throws SQLException, ParseException {
        Connection connShots = shotScraper.setNewConnection("shottest");
        createTestTableForPreparedStatementTesting(connShots, "varchar(100)");
        ArrayList<PreparedStatement> statements = new ArrayList<>();
        statements.add(connShots.prepareStatement("INSERT INTO testtable VALUES (?)"));
        shotScraper.insertParametersIntoAllPreparedStatements(statements, "string", 1, testValue);
        for (PreparedStatement statement : statements) {
            statement.execute();
        }
        ResultSet rs = connShots.prepareStatement("SELECT * FROM testtable").executeQuery();
        HashSet<String> strings = new HashSet<>();
        while (rs.next()) {
            strings.add(rs.getString("testcolumn"));
        }
        HashSet<String> correctStrings = new HashSet<>();
        correctStrings.add(testValue);
        assertEquals(correctStrings, strings);
        rs.close();
        connShots.close();
    }

    /**
     * Provides stream of Strings for testing shouldInsertStringIntoPreparedStatement
     *
     * @return Stream of Strings
     */
    private Stream<Arguments> provideStringsForPreparedStatement() {
        return Stream.of(
                Arguments.of("123456"),
                Arguments.of("test-string"),
                Arguments.of(""),
                Arguments.of("James Johnson"));
    }

    /**
     * Tests that integers are correctly added to prepared statements
     *
     * @param correctInteger integer to test
     * @throws SQLException   If prepared statement fails
     * @throws ParseException If parsing fails
     */
    @ParameterizedTest
    @DisplayName("inserts integers into prepared statement")
    @MethodSource("provideIntegersForPreparedStatement")
    void shouldInsertIntegerIntoPreparedStatement(int correctInteger) throws SQLException, ParseException {
        Connection connShots = shotScraper.setNewConnection("shottest");
        createTestTableForPreparedStatementTesting(connShots, "int");
        ArrayList<PreparedStatement> statements = new ArrayList<>();
        statements.add(connShots.prepareStatement("INSERT INTO testtable VALUES (?)"));
        shotScraper.insertParametersIntoAllPreparedStatements(statements, "integer", 1, correctInteger + "");
        for (PreparedStatement statement : statements) {
            statement.execute();
        }
        ResultSet rs = connShots.prepareStatement("SELECT * FROM testtable").executeQuery();
        HashSet<Integer> integers = new HashSet<>();
        while (rs.next()) {
            integers.add(rs.getInt("testcolumn"));
        }
        HashSet<Integer> correctIntegers = new HashSet<>();
        correctIntegers.add(correctInteger);
        assertEquals(correctIntegers, integers);
        rs.close();
        connShots.close();
    }

    /**
     * Provides stream of integers for testing shouldInsertIntegerIntoPreparedStatement
     *
     * @return Stream of integers
     */
    private Stream<Arguments> provideIntegersForPreparedStatement() {
        return Stream.of(
                Arguments.of(123456),
                Arguments.of(0),
                Arguments.of(Integer.MAX_VALUE));
    }

    /**
     * Tests that dates are correctly added to prepared statements
     *
     * @param inputDate   date as String in the form of YYYYMMDD
     * @param correctDate date as String as it should be stored in database YYYY-MM-DD
     * @throws SQLException   If prepared statement fails
     * @throws ParseException If parsing fails
     */
    @ParameterizedTest
    @DisplayName("inserts dates into prepared statement")
    @MethodSource("provideDatesForPreparedStatement")
    void shouldInsertDateIntoPreparedStatement(String inputDate, String correctDate) throws SQLException, ParseException {
        Connection connShots = shotScraper.setNewConnection("shottest");
        createTestTableForPreparedStatementTesting(connShots, "date");
        ArrayList<PreparedStatement> statements = new ArrayList<>();
        statements.add(connShots.prepareStatement("INSERT INTO testtable VALUES (?)"));
        shotScraper.insertParametersIntoAllPreparedStatements(statements, "date", 1, inputDate);
        for (PreparedStatement statement : statements) {
            statement.execute();
        }
        ResultSet rs = connShots.prepareStatement("SELECT * FROM testtable").executeQuery();
        HashSet<String> dates = new HashSet<>();
        while (rs.next()) {
            dates.add(rs.getDate("testcolumn").toString());
        }
        HashSet<String> correctDates = new HashSet<>();
        correctDates.add(correctDate);
        assertEquals(correctDates, dates);
        rs.close();
        connShots.close();
    }

    /**
     * Provides stream of dates for testing shouldInsertDateIntoPreparedStatement
     *
     * @return Stream of dates
     */
    private Stream<Arguments> provideDatesForPreparedStatement() {
        return Stream.of(
                Arguments.of("20000101", "2000-01-01"),
                Arguments.of("20160229", "2016-02-29"),
                Arguments.of("19971229", "1997-12-29"),
                Arguments.of("20210801", "2021-08-01"),
                Arguments.of("20210802", "2021-08-02"),
                Arguments.of("20210803", "2021-08-03"),
                Arguments.of("20210804", "2021-08-04"),
                Arguments.of("20210805", "2021-08-05"),
                Arguments.of("20210806", "2021-08-06"),
                Arguments.of("20210807", "2021-08-07"),
                Arguments.of("20210808", "2021-08-08"),
                Arguments.of("20210809", "2021-08-09"),
                Arguments.of("20210810", "2021-08-10"),
                Arguments.of("20210811", "2021-08-11"),
                Arguments.of("20210812", "2021-08-12"),
                Arguments.of("20210813", "2021-08-13"),
                Arguments.of("20210814", "2021-08-14"),
                Arguments.of("20210815", "2021-08-15"),
                Arguments.of("20210816", "2021-08-16"),
                Arguments.of("20210817", "2021-08-17"),
                Arguments.of("20210818", "2021-08-18"),
                Arguments.of("20210819", "2021-08-19"),
                Arguments.of("20210820", "2021-08-20"),
                Arguments.of("20210821", "2021-08-21"),
                Arguments.of("20210822", "2021-08-22"),
                Arguments.of("20210823", "2021-08-23"),
                Arguments.of("20210824", "2021-08-24"),
                Arguments.of("20210825", "2021-08-25"),
                Arguments.of("20210826", "2021-08-26"),
                Arguments.of("20210827", "2021-08-27"),
                Arguments.of("20210828", "2021-08-28"),
                Arguments.of("20210829", "2021-08-29"),
                Arguments.of("20210830", "2021-08-30"),
                Arguments.of("20210831", "2021-08-31")
        );
    }

    /**
     * Tests that times are correctly added to prepared statements
     *
     * @param inputTime   time as String in the form of (M)M:SS
     * @param correctTime time as String as it should be stored in database HH:MM:SS
     * @throws SQLException   If prepared statement fails
     * @throws ParseException If parsing fails
     */
    @ParameterizedTest
    @DisplayName("inserts times into prepared statement")
    @MethodSource("provideTimesForPreparedStatement")
    void shouldInsertTimeIntoPreparedStatement(String inputTime, String correctTime) throws SQLException, ParseException {
        Connection connShots = shotScraper.setNewConnection("shottest");
        createTestTableForPreparedStatementTesting(connShots, "time");
        ArrayList<PreparedStatement> statements = new ArrayList<>();
        statements.add(connShots.prepareStatement("INSERT INTO testtable VALUES (?)"));
        shotScraper.insertParametersIntoAllPreparedStatements(statements, "time", 1, inputTime);
        for (PreparedStatement statement : statements) {
            statement.execute();
        }
        ResultSet rs = connShots.prepareStatement("SELECT * FROM testtable").executeQuery();
        HashSet<String> times = new HashSet<>();
        while (rs.next()) {
            times.add(rs.getTime("testcolumn").toString());
        }
        HashSet<String> correctTimes = new HashSet<>();
        correctTimes.add(correctTime);
        assertEquals(correctTimes, times);
        rs.close();
        connShots.close();
    }

    /**
     * Provides stream of times for testing shouldInsertTimeIntoPreparedStatement
     *
     * @return Stream of dates
     */
    private Stream<Arguments> provideTimesForPreparedStatement() {
        return Stream.of(
                Arguments.of("0:00", "00:00:00"),
                Arguments.of("0:09", "00:00:09"),
                Arguments.of("1:10", "00:01:10"),
                Arguments.of("11:45", "00:11:45"));
    }

    /**
     * Creates a test table in test database for testing if different data types are added correctly
     *
     * @param conn     connection to test database
     * @param datatype the type of data to be added
     * @throws SQLException If creating table fails
     */
    private void createTestTableForPreparedStatementTesting(Connection conn, String datatype) throws SQLException {
        String createTestShotTable = "CREATE TABLE IF NOT EXISTS testtable (\n" +
                "`testcolumn` " + datatype + " NOT NULL)\n"
                + "ENGINE=InnoDB\n"
                + "DEFAULT CHARSET=utf8mb4\n"
                + "COLLATE=utf8mb4_0900_ai_ci";
        conn.prepareStatement(createTestShotTable).execute();
    }

    /**
     * Tests that real shots (sampled from real response data) are inserted correctly into database
     *
     * @throws IOException   If reading sample data file fails
     * @throws JSONException If parsing JSON fails
     * @throws SQLException  If inserting data or querying database fails
     */
    @Test
    @DisplayName("inserts all shots to new table")
    void shouldInsertRealShotsToNewTable() throws IOException, JSONException, SQLException {
        Connection connPlayers = allTeamAndPlayerScraper.setNewConnection("playertest");
        //Create test player tables
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(connPlayers, "playertest");
        //Sample team data location
        String[] teams = Files.readString(Path.of("src/main/resources/getAllTeamAndPlayerDataSampleResponse.txt"), StandardCharsets.US_ASCII)
                .split("\"teams\"")[1]
                .split("\"players\"")[0]
                .split("\\]\\]");
        //Add teams to database
        allTeamAndPlayerScraper.processTeamData(teams, connPlayers, connPlayers);
        //Sample shot data
        String response = Files.readString(Path.of("src/main/resources/TonyParker2018-19PreseasonSampleShotData.txt"), StandardCharsets.US_ASCII);
        JSONArray rowSets = new JSONObject(response).getJSONArray("resultSets").getJSONObject(0).getJSONArray("rowSet");
        shotScraper = new ShotScraper("shottest", "shottest", "playertest", "playertest", new IndividualPlayerScraper("playertest", "playertest"));
        Connection connShots = shotScraper.setNewConnection("shottest");
        //Create test shot tables
        shotScraper.createAllShotsTable(connShots, connShots);
        shotScraper.createIndividualSeasonTable("Parker_Tony_2225_2018_19_Preseason", connShots, connShots);
        //Insert shots scraped from sample data
        shotScraper.insertShots("Parker_Tony_2225_2018_19_Preseason", "Tony", "Parker", "2018-19", "Preseason", rowSets, new HashSet<>(), connShots, connShots);
        //Create set of shots that should be inserted into database
        HashSet<Shot> correctShots = createSetOfCorrectShots();
        //Find shot data that was just inserted
        ResultSet shotResultSet = connShots.prepareStatement("SELECT * FROM Parker_Tony_2225_2018_19_Preseason").executeQuery();
        HashSet<Shot> retrievedShots = new HashSet<>();
        while (shotResultSet.next()) {
            retrievedShots.add(new Shot(
                    shotResultSet.getString("playerlast"),
                    shotResultSet.getString("playerfirst"),
                    shotResultSet.getString("season"),
                    shotResultSet.getString("seasontype"),
                    shotResultSet.getDate("calendar").toString(),
                    shotResultSet.getTime("clock").toString(),
                    shotResultSet.getString("shottype"),
                    shotResultSet.getString("playtype"),
                    shotResultSet.getString("teamname"),
                    shotResultSet.getString("awayteamname"),
                    shotResultSet.getString("hometeamname"),
                    shotResultSet.getString("shotzonebasic"),
                    shotResultSet.getString("shotzonearea"),
                    shotResultSet.getString("shotzonerange"),
                    shotResultSet.getInt("playerid"),
                    shotResultSet.getInt("gameid"),
                    shotResultSet.getInt("gameeventid"),
                    shotResultSet.getInt("minutes"),
                    shotResultSet.getInt("seconds"),
                    shotResultSet.getInt("x"),
                    shotResultSet.getInt("y"),
                    shotResultSet.getInt("distance"),
                    shotResultSet.getInt("make"),
                    shotResultSet.getInt("period"),
                    shotResultSet.getInt("teamid"),
                    shotResultSet.getInt("awayteamid"),
                    shotResultSet.getInt("hometeamid"),
                    shotResultSet.getInt("athome")));
        }
        assertEquals(correctShots, retrievedShots);
        shotResultSet.close();
        connShots.close();
        connPlayers.close();
    }

    /**
     * Tests that new shots are added when there are already existing shots in the database
     *
     * @throws IOException   If reading sample data file fails
     * @throws JSONException If parsing JSON fails
     * @throws SQLException  If inserting data or querying database fails
     */
    @Test
    @DisplayName("inserts new shots to table with existing shots")
    void shouldInsertRealShotsToTableWithExistingShots() throws IOException, JSONException, SQLException {
        Connection connPlayers = allTeamAndPlayerScraper.setNewConnection("playertest");
        //Create test player tables
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(connPlayers, "playertest");
        //Sample team data
        String[] teams = Files.readString(Path.of("src/main/resources/getAllTeamAndPlayerDataSampleResponse.txt"), StandardCharsets.US_ASCII)
                .split("\"teams\"")[1]
                .split("\"players\"")[0]
                .split("\\]\\]");
        allTeamAndPlayerScraper.processTeamData(teams, connPlayers, connPlayers);
        //Sample shot data
        String response = Files.readString(Path.of("src/main/resources/TonyParker2018-19PreseasonSampleShotData.txt"), StandardCharsets.US_ASCII);
        JSONArray rowSets = new JSONObject(response).getJSONArray("resultSets").getJSONObject(0).getJSONArray("rowSet");
        shotScraper = new ShotScraper("shottest", "shottest", "playertest", "playertest", new IndividualPlayerScraper("playertest", "playertest"));
        Connection connShots = shotScraper.setNewConnection("shottest");
        //Create test shot tables
        shotScraper.createAllShotsTable(connShots, connShots);
        shotScraper.createIndividualSeasonTable("Parker_Tony_2225_2018_19_Preseason", connShots, connShots);
        //Insert one shot present in sample data before parsing sample data
        connShots.prepareStatement("INSERT INTO Parker_Tony_2225_2018_19_Preseason VALUES ('2225-11800002-105', 2225, 'Parker', 'Tony', '2018-19', 'Preseason', 11800002, " +
                "105, '2018-09-28', '00:03:41', 3, 41, 83, 190, 20, 0, 1, '2PT Field Goal', 'Jump Shot' , 1610612766, 'Charlotte Hornets', 1610612738, 'BOS', 1610612766," +
                " 'CHA', 1, 'Mid-Range', 'Right Side Center(RC)', '16-24 ft.')").execute();
        //Insert shots from sample data
        shotScraper.insertShots("Parker_Tony_2225_2018_19_Preseason", "Tony", "Parker", "2018-19", "Preseason", rowSets, new HashSet<>(Set.of("2225-11800002-105")), connShots, connShots);
        ResultSet shotResultSet = connShots.prepareStatement("SELECT * FROM Parker_Tony_2225_2018_19_Preseason").executeQuery();
        HashSet<Shot> correctShots = createSetOfCorrectShots();
        HashSet<Shot> retrievedShots = new HashSet<>();
        while (shotResultSet.next()) {
            retrievedShots.add(new Shot(
                    shotResultSet.getString("playerlast"),
                    shotResultSet.getString("playerfirst"),
                    shotResultSet.getString("season"),
                    shotResultSet.getString("seasontype"),
                    shotResultSet.getDate("calendar").toString(),
                    shotResultSet.getTime("clock").toString(),
                    shotResultSet.getString("shottype"),
                    shotResultSet.getString("playtype"),
                    shotResultSet.getString("teamname"),
                    shotResultSet.getString("awayteamname"),
                    shotResultSet.getString("hometeamname"),
                    shotResultSet.getString("shotzonebasic"),
                    shotResultSet.getString("shotzonearea"),
                    shotResultSet.getString("shotzonerange"),
                    shotResultSet.getInt("playerid"),
                    shotResultSet.getInt("gameid"),
                    shotResultSet.getInt("gameeventid"),
                    shotResultSet.getInt("minutes"),
                    shotResultSet.getInt("seconds"),
                    shotResultSet.getInt("x"),
                    shotResultSet.getInt("y"),
                    shotResultSet.getInt("distance"),
                    shotResultSet.getInt("make"),
                    shotResultSet.getInt("period"),
                    shotResultSet.getInt("teamid"),
                    shotResultSet.getInt("awayteamid"),
                    shotResultSet.getInt("hometeamid"),
                    shotResultSet.getInt("athome")));
        }
        assertEquals(correctShots, retrievedShots);
        shotResultSet.close();
        connShots.close();
    }

    /**
     * Drops all tables in the test database after all tests are complete
     *
     * @throws SQLException If dropping tables fails
     */
    @AfterEach
    void dropAllTablesAfter() throws SQLException {
        Connection connPlayers = shotScraper.setNewConnection("playertest");
        ResultSet rsTables = connPlayers.prepareStatement("SHOW TABLES").executeQuery();
        while (rsTables.next()) {
            connPlayers.prepareStatement("DROP TABLE " + rsTables.getString(1)).execute();
        }
        rsTables.close();
        connPlayers.close();
        Connection connShots = shotScraper.setNewConnection("shottest");
        ResultSet rsTablesShots = connShots.prepareStatement("SHOW TABLES").executeQuery();
        while (rsTablesShots.next()) {
            connShots.prepareStatement("DROP TABLE " + rsTablesShots.getString(1)).execute();
        }
        rsTablesShots.close();
        connShots.close();
    }

    /**
     * Creates HashSet of shots that should be inserted into database when testing
     *
     * @return set of shots
     */
    private HashSet<Shot> createSetOfCorrectShots() {
        HashSet<Shot> correctShots = new HashSet<>();
        new HashSet<>();
        correctShots.add(new Shot("Parker",
                "Tony",
                "2018-19",
                "Preseason",
                "2018-09-28",
                "00:03:41",
                "2PT Field Goal",
                "Jump Shot",
                "Charlotte Hornets",
                "BOS",
                "CHA",
                "Mid-Range",
                "Right Side Center(RC)",
                "16-24 ft.",
                2225,
                11800002,
                105,
                3,
                41,
                83,
                190,
                20,
                0,
                1,
                1610612766,
                1610612738,
                1610612766,
                1));
        correctShots.add(new Shot("Parker",
                "Tony",
                "2018-19",
                "Preseason",
                "2018-09-28",
                "00:00:19",
                "2PT Field Goal",
                "Jump Shot",
                "Charlotte Hornets",
                "BOS",
                "CHA",
                "Mid-Range",
                "Right Side Center(RC)",
                "16-24 ft.",
                2225,
                11800002,
                146,
                0,
                19,
                116,
                169,
                20,
                0,
                1,
                1610612766,
                1610612738,
                1610612766,
                1));
        correctShots.add(new Shot("Parker",
                "Tony",
                "2018-19",
                "Preseason",
                "2018-10-12",
                "00:11:37",
                "2PT Field Goal",
                "Driving Floating Jump Shot",
                "Charlotte Hornets",
                "CHA",
                "DAL",
                "Mid-Range",
                "Left Side Center(LC)",
                "16-24 ft.",
                2225,
                11800071,
                168,
                11,
                37,
                -149,
                153,
                21,
                0,
                2,
                1610612766,
                1610612766,
                1610612742,
                0));
        return correctShots;
    }
}
