package com.example.ShotScraperV2;

import com.example.ShotScraperV2.objects.Player;
import com.example.ShotScraperV2.objects.Team;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * Tests for AllTeamAndPlayerScraper
 */
@SpringBootTest
@DisplayName("AllTeamAndPlayerScraper")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AllTeamAndPlayerScraperTests implements ScraperUtilsInterface {

    @Autowired
    AllTeamAndPlayerScraper allTeamAndPlayerScraper;

    /**
     * Clears test database before testing
     *
     * @throws SQLException If statement fails
     */
    @BeforeAll
    void dropAllTablesBefore() throws SQLException {
        Connection connPlayers = ScraperUtilsInterface.super.setNewConnection("playertest");
        ResultSet rsTables = connPlayers.prepareStatement("SHOW TABLES").executeQuery();
        while (rsTables.next()) {
            connPlayers.prepareStatement("DROP TABLE " + rsTables.getString(1)).execute();
        }
        rsTables.close();
        connPlayers.close();
    }

    /**
     * Checks only general tables are created
     *
     * @throws SQLException If query fails
     */
    @Test
    @DisplayName("creates all general tables")
    void shouldCreateGeneralTables() throws SQLException {
        Connection connPlayers = ScraperUtilsInterface.super.setNewConnection("playertest");
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(connPlayers, "playertest");
        ResultSet rsTables = connPlayers.prepareStatement("SHOW TABLES").executeQuery();
        HashSet<String> tableNames = new HashSet<>();
        while (rsTables.next()) {
            tableNames.add(rsTables.getString(1));
        }
        rsTables.close();
        connPlayers.close();
        assertAll("Should return created general tables",
                () -> assertTrue(tableNames.contains("player_all_data")),
                () -> assertTrue(tableNames.contains("player_relevant_data")),
                () -> assertTrue(tableNames.contains("team_data")),
                () -> assertTrue(tableNames.contains("misc")),
                () -> assertEquals(4, tableNames.size()));
    }

    /**
     * Verifies the correct activity indexes are found from a given array
     *
     * @param expected      array which should be returned
     * @param playerDetails provided array of player details
     */
    @ParameterizedTest
    @MethodSource("provideActivityIndexes")
    @DisplayName("finds the correct activity indexes")
    void shouldFindCorrectActivityIndex(int[] expected, String[] playerDetails) {
        assertArrayEquals(expected, allTeamAndPlayerScraper.findImportantIndexes(playerDetails));
    }

    /**
     * Provides arguments to shouldFindCorrectActivityIndex
     *
     * @return stream of arguments
     */
    private static Stream<Arguments> provideActivityIndexes() {
        return Stream.of(
                Arguments.of(new int[]{3, 4, 5}, new String[]{"12345", "Bar", "Foo", "1", "2015", "2020", "unknown", "CHI"}),
                Arguments.of(new int[]{3, 4, 5}, new String[]{"12345", "Bar", "Foo", "0", "2015", "2020", "unknown"}),
                Arguments.of(new int[]{2, 3, 4}, new String[]{"12345", "Foo", "0", "2015", "2020", "unknown"}),
                Arguments.of(new int[]{4, 5, 6}, new String[]{"12345", "Bar", "Boo", "Foo", "0", "2015", "2020", "unknown"}));
    }

    /**
     * Tests if insertPlayerData method can adapt to players with 1, 2, or 3 names in their full name
     *
     * @param player player object to be inserted into database
     * @throws SQLException If querying the database fails
     */
    @ParameterizedTest
    @DisplayName("inserts players with varying name length")
    @MethodSource("providePlayersWithVariedNameLength")
    void shouldInsertPlayerDataWithVariedNameLength(Player player) throws SQLException {
        Connection conn = ScraperUtilsInterface.super.setNewConnection("playertest");
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(conn, "playertest");
        String sqlInsert = "INSERT INTO player_all_data (id, lastname ,firstname,firstactiveyear,mostrecentactiveyear,currentlyactive) VALUES (?,?,?,?,?,?) ";
        allTeamAndPlayerScraper.insertPlayerData(sqlInsert, player.generatePlayerDetailsArray(), conn, allTeamAndPlayerScraper.findImportantIndexes(player.generatePlayerDetailsArray())[0]);
        ResultSet rs = conn.prepareStatement("SELECT * FROM player_all_data").executeQuery();
        while (rs.next()) {
            if (rs.getString("firstname").length() != 0) {
                assertEquals(player, new Player(rs.getString("id"), rs.getString("lastname"), rs.getString("firstname"), rs.getString("currentlyactive"), rs.getString("firstactiveyear").substring(0, 4), rs.getString("mostrecentactiveyear").substring(0, 4)));
            } else {
                assertEquals(player, new Player(rs.getString("id"), rs.getString("lastname"), rs.getString("currentlyactive"), rs.getString("firstactiveyear").substring(0, 4), rs.getString("mostrecentactiveyear").substring(0, 4)));
            }
        }
        rs.close();
        conn.close();
    }

    /**
     * Provides Player objects as inputs with varying name length into shouldInsertPlayerDataWithVariedNameLength
     *
     * @return stream of Players
     */

    private static Stream<Arguments> providePlayersWithVariedNameLength() {
        return Stream.of(
                Arguments.of(new Player("12", "Bar", "Foo", "1", "2015", "2020")),
                Arguments.of(new Player("13", "Foo", "0", "2015", "2020")),
                Arguments.of(new Player("14", "Bar", "Boo", "Foo", "0", "2015", "2020")));
    }

    /**
     * Tests that teams are added to the database, with teams having varying amounts of data
     *
     * @throws IOException  If sample response file cannot be found
     * @throws SQLException If database queries fail
     */
    @Test
    @DisplayName("saves all teams into database")
    void shouldAddAllTeamsIntoDatabase() throws IOException, SQLException {
        //Use sample response
        String[] teams = Files.readString(Path.of("src/main/resources/getAllTeamAndPlayerDataSampleResponse.txt"), StandardCharsets.US_ASCII)
                .split("\"teams\"")[1]
                .split("\"players\"")[0]
                .split("\\]\\]");
        Connection conn = ScraperUtilsInterface.super.setNewConnection("playertest");
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(conn, "playertest");
        allTeamAndPlayerScraper.processTeamData(teams, conn, conn);
        int teamCounter = 0;
        //Teams may be missing an abbreviation or casualName, test all three cases
        Team normalTeamLength = new Team(12321, "FBU", "fenerbahce_ulker", "Istanbul", "Fenerbahce Ulker");
        Team noAbbrTeam = new Team(1610610023, "", "anderson_packers", "Anderson", "Packers");
        Team noCasualNameTeam = new Team(1610610024, "BAL", "", "Baltimore", "Bullets");
        HashSet<Team> savedTeams = new HashSet<>();
        ResultSet allTeams = conn.prepareStatement("SELECT * FROM team_data").executeQuery();
        while (allTeams.next()) {
            teamCounter++;
            savedTeams.add(new Team(allTeams.getInt("id"), allTeams.getString("abbr")
                    , allTeams.getString("casualname"), allTeams.getString("firstname"), allTeams.getString("secondname")));
        }
        allTeams.close();
        conn.close();
        //Should be 78 teams
        assertEquals(78, teamCounter);
        assertAll("Should contain sample teams",
                () -> assertTrue(savedTeams.contains(normalTeamLength)),
                () -> assertTrue(savedTeams.contains(noAbbrTeam)),
                () -> assertTrue(savedTeams.contains(noCasualNameTeam)));
    }

            /*
        :[[76001,"Abdelnaby, Alaa",0,1990,1994,0,""
,[76002,"Abdul-Aziz, Zaid",0,1968,1977,0,""
,[76003,"Abdul-Jabbar, Kareem",0,1969,1988,0,""
,[51,"Abdul-Rauf, Mahmoud",0,1990,2000,0,""
,[1505,"Abdul-Wahad, Tariq",0,1997,2003,0,""
,[949,"Abdur-Rahim, Shareef",0,1996,2007,0,""
,[76005,"Abernethy, Tom",0,1976,1980,0,""
...
,[78648,"Zopf, Bill",0,1970,1970,0,""
,[1627826,"Zubac, Ivica",1,2016,2021,0,"clippers"
,[78650,"Zunic, Matt",0,1948,1948,0,""

}};
         */

    /**
     * Tests that provided sample of players will be added to the all player table
     *
     * @param correctPlayers array of players that should exist in the table
     * @param players        input of player data to be added
     * @throws SQLException If query fails
     */
    @ParameterizedTest
    @DisplayName("adds new players into all players table")
    @MethodSource("provideNewPlayersForAllPlayersTable")
    void shouldAddNewPlayersToAllPlayersTable(ArrayList<Player> correctPlayers, String[] players) throws SQLException {
        Connection conn = ScraperUtilsInterface.super.setNewConnection("playertest");
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(conn, "playertest");
        allTeamAndPlayerScraper.processPlayerData(players, new HashMap<>(), new HashMap<>(), conn, conn);
        ResultSet playerRS = conn.prepareStatement("SELECT * FROM player_all_data").executeQuery();
        HashSet<Player> allPlayers = new HashSet<>();
        while (playerRS.next()) {
            allPlayers.add(new Player(playerRS.getInt("id") + "", playerRS.getString("lastname"), playerRS.getString("firstname"),
                    playerRS.getInt("currentlyactive") + "", playerRS.getString("firstactiveyear").substring(0, 4), playerRS.getString("mostrecentactiveyear").substring(0, 4)));
        }
        conn.close();
        playerRS.close();
        for (Player eachPlayer : correctPlayers) {
            assertTrue(allPlayers.contains(eachPlayer));
        }
    }

    /**
     * Provides test values for shouldAddNewPlayersToAllPlayersTable
     *
     * @return stream of test arguments
     */
    private static Stream<Arguments> provideNewPlayersForAllPlayersTable() {
        return Stream.of(
                Arguments.of(new ArrayList<>(Arrays.asList(new Player("100", "Doe", "John", "0", "2000", "2001"),
                                new Player("101", "Smith", "Jane", "1", "2019", "2020"),
                                new Player("102", "Jones", "Bob", "0", "1950", "1970"))),
                        new String[]{":[[100,\"Doe, John\",0,2000,2001,0,\"\"",
                                ",[101,\"Smith, Jane\",1,2019,2020,0,\"clippers\"",
                                ",[102,\"Jones, Bob\",0,1950,1970,0,\"\""}));
    }

    /**
     * Tests that only sample players active after 1996 should be added to relevant players table
     *
     * @param correctPlayers array of players that should exist in the table
     * @param wrongPlayers   array of players that should not exist in the table
     * @param players        input of player data to be added
     * @throws SQLException If query fails
     */
    @ParameterizedTest
    @DisplayName("adds appropriate players into relevant players table")
    @MethodSource("provideNewPlayersForRelevantPlayersTable")
    void shouldAddNewPlayersToRelevantPlayersTable(ArrayList<Player> correctPlayers, ArrayList<Player> wrongPlayers, String[] players) throws SQLException {
        Connection conn = ScraperUtilsInterface.super.setNewConnection("playertest");
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(conn, "playertest");
        allTeamAndPlayerScraper.processPlayerData(players, new HashMap<>(), new HashMap<>(), conn, conn);
        ResultSet playerRS = conn.prepareStatement("SELECT * FROM player_relevant_data").executeQuery();
        HashSet<Player> allPlayers = new HashSet<>();
        while (playerRS.next()) {
            allPlayers.add(new Player(playerRS.getInt("id") + "", playerRS.getString("lastname"), playerRS.getString("firstname"),
                    playerRS.getInt("currentlyactive") + "", playerRS.getString("firstactiveyear").substring(0, 4), playerRS.getString("mostrecentactiveyear").substring(0, 4)));
        }
        conn.close();
        playerRS.close();
        for (Player eachPlayer : correctPlayers) {
            assertTrue(allPlayers.contains(eachPlayer));
        }
        for (Player eachPlayer : wrongPlayers) {
            assertFalse(allPlayers.contains(eachPlayer));
        }
    }

    /**
     * Provides input arguments to shouldAddNewPlayersToRelevantPlayersTable
     *
     * @return stream of test arguments
     */
    private static Stream<Arguments> provideNewPlayersForRelevantPlayersTable() {
        return Stream.of(
                Arguments.of(new ArrayList<>(Arrays.asList(new Player("100", "Doe", "John", "0", "2000", "2001"),
                                new Player("101", "Smith", "Jane", "1", "2019", "2020"))),
                        new ArrayList<>(Arrays.asList(new Player("100", "Jones", "Bob", "0", "1950", "1970"))),
                        new String[]{":[[100,\"Doe, John\",0,2000,2001,0,\"\"",
                                ",[101,\"Smith, Jane\",1,2019,2020,0,\"clippers\"",
                                ",[102,\"Jones, Bob\",0,1950,1970,0,\"\""}));
    }

    /**
     * Tests that player activity will be updated
     *
     * @param correctPlayers array of players that should exist in the table
     * @param players        input of player data to be added
     * @throws SQLException If query fails
     */
    @ParameterizedTest
    @DisplayName("updates player activity")
    @MethodSource("providePlayersToUpdateActivity")
    void shouldUpdatePlayerActivity(ArrayList<Player> correctPlayers, String[] players) throws SQLException {
        Connection conn = ScraperUtilsInterface.super.setNewConnection("playertest");
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(conn, "playertest");
        //Setup database with values that need to be updated
        setupDatabaseForUpdatingPlayers(conn);
        allTeamAndPlayerScraper.processPlayerData(players, new HashMap<>(), new HashMap<>(), conn, conn);
        ResultSet playerRS = conn.prepareStatement("SELECT * FROM player_relevant_data").executeQuery();
        HashSet<Player> allPlayers = new HashSet<>();
        while (playerRS.next()) {
            allPlayers.add(new Player(playerRS.getInt("id") + "", playerRS.getString("lastname"), playerRS.getString("firstname"),
                    playerRS.getInt("currentlyactive") + "", playerRS.getString("firstactiveyear").substring(0, 4), playerRS.getString("mostrecentactiveyear").substring(0, 4)));
        }
        conn.close();
        playerRS.close();
        for (Player eachPlayer : correctPlayers) {
            assertTrue(allPlayers.contains(eachPlayer));
        }
    }

    /**
     * Provides input arguments to shouldUpdatePlayerActivity
     *
     * @return stream of test arguments
     */
    private static Stream<Arguments> providePlayersToUpdateActivity() {
        return Stream.of(
                Arguments.of(new ArrayList<>(Arrays.asList(new Player("100", "Doe", "John", "1", "2000", "2020"),
                                new Player("101", "Smith", "Jane", "0", "2019", "2020"))),
                        new String[]{":[[100,\"Doe, John\",1,2000,2020,0,\"\"",
                                ",[101,\"Smith, Jane\",0,2019,2020,0,\"clippers\""}));
    }

    /**
     * Tests that player most recent active year will be updated
     *
     * @param correctPlayers array of players that should exist in the table
     * @param players        input of player data to be added
     * @throws SQLException If query fails
     */
    @ParameterizedTest
    @DisplayName("updates player activity")
    @MethodSource("providePlayersToUpdateMostRecentActiveYear")
    void shouldUpdatePlayerMostRecentActiveYear(ArrayList<Player> correctPlayers, String[] players) throws SQLException {
        Connection conn = ScraperUtilsInterface.super.setNewConnection("playertest");
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(conn, "playertest");
        //Setup database with values that need to be updated
        setupDatabaseForUpdatingPlayers(conn);
        allTeamAndPlayerScraper.processPlayerData(players, new HashMap<>(), new HashMap<>(), conn, conn);
        ResultSet playerRS = conn.prepareStatement("SELECT * FROM player_relevant_data").executeQuery();
        HashSet<Player> allPlayers = new HashSet<>();
        while (playerRS.next()) {
            allPlayers.add(new Player(playerRS.getInt("id") + "", playerRS.getString("lastname"), playerRS.getString("firstname"),
                    playerRS.getInt("currentlyactive") + "", playerRS.getString("firstactiveyear").substring(0, 4), playerRS.getString("mostrecentactiveyear").substring(0, 4)));
        }
        conn.close();
        playerRS.close();
        for (Player eachPlayer : correctPlayers) {
            assertTrue(allPlayers.contains(eachPlayer));
        }
    }

    /**
     * Provides input arguments to shouldUpdatePlayerMostRecentActiveYear
     *
     * @return stream of test arguments
     */
    private static Stream<Arguments> providePlayersToUpdateMostRecentActiveYear() {
        return Stream.of(
                Arguments.of(new ArrayList<>(Arrays.asList(new Player("100", "Doe", "John", "0", "2000", "2020"),
                                new Player("101", "Smith", "Jane", "1", "2019", "2020"))),
                        new String[]{":[[100,\"Doe, John\",0,2000,2020,0,\"\"",
                                ",[101,\"Smith, Jane\",1,2019,2020,0,\"clippers\""}));
    }


    /**
     * Sets up database with wrong values that are to be updated
     *
     * @param conn connection to database
     * @throws SQLException If statement fails
     */
    private void setupDatabaseForUpdatingPlayers(Connection conn) throws SQLException {
        String[] tables = new String[]{"player_all_data", "player_relevant_data"};
        for (String eachTable : tables) {
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + eachTable + " VALUES (?,?,?,?,?,?)");
            stmt.setInt(1, 100);
            stmt.setString(2, "Doe");
            stmt.setString(3, "John");
            stmt.setString(4, "2000-01");
            stmt.setString(5, "2001-02");
            stmt.setInt(6, 0);
            stmt.execute();
            conn.prepareStatement("INSERT INTO " + eachTable + " VALUES (?,?,?,?,?,?)");
            stmt.setInt(1, 101);
            stmt.setString(2, "Smith");
            stmt.setString(3, "Jane");
            stmt.setString(4, "2019-20");
            stmt.setString(5, "2019-20");
            stmt.setInt(6, 1);
            stmt.execute();
        }
    }

    /**
     * Drops all tables in the test database after all tests are complete
     *
     * @throws SQLException If dropping tables fails
     */
    @AfterEach
    void dropAllTablesAfter() throws SQLException {
        Connection connPlayers = ScraperUtilsInterface.super.setNewConnection("playertest");
        ResultSet rsTables = connPlayers.prepareStatement("SHOW TABLES").executeQuery();
        while (rsTables.next()) {
            connPlayers.prepareStatement("DROP TABLE " + rsTables.getString(1)).execute();
        }
        rsTables.close();
        connPlayers.close();
    }
}
