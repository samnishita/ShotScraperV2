package com.example.ShotScraperV2;

import com.example.ShotScraperV2.nbaobjects.Player;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest
@DisplayName("RunHandler")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RunHandlerTests implements ScraperUtilsInterface {
    @Autowired
    RunHandler runHandler;
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

    @Test
    void shouldPopulateQueueWithAllPlayers() throws SQLException {
        Connection conn = ScraperUtilsInterface.super.setNewConnection("playertest");
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(conn, "playertest");
        setupDatabaseWithPlayers(conn);
        HashSet<Player> correctPlayers = new HashSet<>();
        HashSet<Player> polledPlayers = new HashSet<>();
        correctPlayers.add(new Player("100", "Doe", "John", "0", "2000-01", "2001-02"));
        correctPlayers.add(new Player("101", "Smith", "Jane", "1", "2019-20", "2019-20"));
        correctPlayers.add(new Player("102", "Jones", "Bob", "0", "2018-19", "2020-21"));
        correctPlayers.add(new Player("103", "Jackson", "Steve", "1", "2019-20", "2020-21"));
        runHandler.populateThreadSafeQueueWithPlayers(conn, false, false, false, conn.getSchema());
        while (true) {
            Player polledPlayer = RunHandler.pollQueue();
            if (polledPlayer == null) {
                break;
            }
            polledPlayers.add(polledPlayer);
        }
        assertEquals(correctPlayers,polledPlayers);
        conn.close();
    }

    @Test
    void shouldPopulateQueueWithOnlyActivePlayers() throws SQLException {
        Connection conn = ScraperUtilsInterface.super.setNewConnection("playertest");
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(conn, "playertest");
        setupDatabaseWithPlayers(conn);
        HashSet<Player> correctPlayers = new HashSet<>();
        HashSet<Player> polledPlayers = new HashSet<>();
        correctPlayers.add(new Player("101", "Smith", "Jane", "1", "2019-20", "2019-20"));
        correctPlayers.add(new Player("103", "Jackson", "Steve", "1", "2019-20", "2020-21"));
        runHandler.populateThreadSafeQueueWithPlayers(conn, true, false, false, conn.getSchema());
        while (true) {
            Player polledPlayer = RunHandler.pollQueue();
            if (polledPlayer == null) {
                break;
            }
            polledPlayers.add(polledPlayer);
        }
        assertEquals(correctPlayers,polledPlayers);
        conn.close();
    }

    @Test
    void shouldPopulateQueueWithOnlyPlayersMostRecentYearIsCurrentYear() throws SQLException {
        Connection conn = ScraperUtilsInterface.super.setNewConnection("playertest");
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(conn, "playertest");
        setupDatabaseWithPlayers(conn);
        HashSet<Player> correctPlayers = new HashSet<>();
        HashSet<Player> polledPlayers = new HashSet<>();
        correctPlayers.add(new Player("102", "Jones", "Bob", "0", "2018-19", "2020-21"));
        correctPlayers.add(new Player("103", "Jackson", "Steve", "1", "2019-20", "2020-21"));
        runHandler.populateThreadSafeQueueWithPlayers(conn, false, true, false, conn.getSchema());
        while (true) {
            Player polledPlayer = RunHandler.pollQueue();
            if (polledPlayer == null) {
                break;
            }
            polledPlayers.add(polledPlayer);
        }
        assertEquals(correctPlayers,polledPlayers);
        conn.close();
    }

    @Test
    void shouldPopulateQueueWithOnlyPlayersActiveInCurrentYear() throws SQLException {
        Connection conn = ScraperUtilsInterface.super.setNewConnection("playertest");
        allTeamAndPlayerScraper.createGeneralTablesIfNecessary(conn, "playertest");
        setupDatabaseWithPlayers(conn);
        HashSet<Player> correctPlayers = new HashSet<>();
        HashSet<Player> polledPlayers = new HashSet<>();
        correctPlayers.add(new Player("103", "Jackson", "Steve", "1", "2019-20", "2020-21"));
        runHandler.populateThreadSafeQueueWithPlayers(conn, true, true, false, conn.getSchema());
        while (true) {
            Player polledPlayer = RunHandler.pollQueue();
            if (polledPlayer == null) {
                break;
            }
            polledPlayers.add(polledPlayer);
        }
        assertEquals(correctPlayers,polledPlayers);
        conn.close();
    }

    /**
     * Sets up database with known values
     *
     * @param conn connection to database
     * @throws SQLException If statement fails
     */
    private void setupDatabaseWithPlayers(Connection conn) throws SQLException {
        String[] tables = new String[]{"player_all_data", "player_relevant_data"};
        for (String eachTable : tables) {
            //Not active, not current year
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + eachTable + " VALUES (?,?,?,?,?,?)");
            stmt.setInt(1, 100);
            stmt.setString(2, "Doe");
            stmt.setString(3, "John");
            stmt.setString(4, "2000-01");
            stmt.setString(5, "2001-02");
            stmt.setInt(6, 0);
            stmt.execute();
            //Active, not current year
            conn.prepareStatement("INSERT INTO " + eachTable + " VALUES (?,?,?,?,?,?)");
            stmt.setInt(1, 101);
            stmt.setString(2, "Smith");
            stmt.setString(3, "Jane");
            stmt.setString(4, "2019-20");
            stmt.setString(5, "2019-20");
            stmt.setInt(6, 1);
            stmt.execute();
            //Not active, is current year
            conn.prepareStatement("INSERT INTO " + eachTable + " VALUES (?,?,?,?,?,?)");
            stmt.setInt(1, 102);
            stmt.setString(2, "Jones");
            stmt.setString(3, "Bob");
            stmt.setString(4, "2018-19");
            stmt.setString(5, "2020-21");
            stmt.setInt(6, 0);
            stmt.execute();
            //Active, current year
            conn.prepareStatement("INSERT INTO " + eachTable + " VALUES (?,?,?,?,?,?)");
            stmt.setInt(1, 103);
            stmt.setString(2, "Jackson");
            stmt.setString(3, "Steve");
            stmt.setString(4, "2019-20");
            stmt.setString(5, "2020-21");
            stmt.setInt(6, 1);
            stmt.execute();
        }
    }

    /**
     * Drops all tables in the test database after all tests are complete
     *
     * @throws SQLException
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
