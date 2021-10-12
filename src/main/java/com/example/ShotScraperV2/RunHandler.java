package com.example.ShotScraperV2;

import com.example.ShotScraperV2.nbaobjects.Player;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What parts of the scraper should run
 */
@Component
public class RunHandler implements ScraperUtilsInterface, ApplicationListener<ApplicationReadyEvent> {
    private Logger LOGGER = LoggerFactory.getLogger(RunHandler.class);

    //Player scraper choices
    /**
     * Get basic team and player data
     */
    private boolean getTeamAndPlayerData = true;
    /**
     * Get detailed player data for all players for the first time
     */
    private boolean getAllPlayersForFirstTime = false;
    /**
     * Get detailed player data for only players active in the current year and season type
     */
    private boolean updatePlayersForCurrentYearOnly = true;
    /**
     * Group active players for each season
     */
    private boolean organizePlayersByYear = false;

    //Shot scraper choices
    /**
     * Get detailed shot data for all players for first time
     */
    private boolean getAllShotsForFirstTime = false;
    /**
     * Get detailed shot data for only players active in the current year and season type
     */
    private boolean updateShotsForCurrentYear = true;
    /**
     * Calculate the shot percentage for spaces on the court used for hex maps. Uses the OFFSET parameter to determine the size of the spaces
     */
    private boolean makeShotLocationAverages = false;
    /**
     * Calculate the shot percentage for zones on the court used for zone maps
     */
    private boolean makeZoneAverages = false;
    /**
     * Calculate the shot percentage for each distance from the basket
     */
    private boolean makeDistanceAverages = false;
    /**
     * Find all different types of shots present in the database
     */
    private boolean makePlayTypeTable = false;
    /**
     * Will drop empty shot tables if a rerun is needed
     */
    private boolean dropAllEmptyShotTables = false;

    //Misc scraper choices
    /**
     * Verify every player entry is consistent between databases
     */
    private boolean doubleCheckPlayerTables = false;
    /**
     * Verify every shot entry is consistent between databases
     */
    private boolean doubleCheckShotTables = true;
    private boolean checkFullShots = false;
    /**
     * Drop tables with discrepancies to rerun
     */
    private boolean dropMismatchedTables = false;
    /**
     * The present season type
     */
    private String seasonType = "preseason";// reg, preseason, or playoffs
    /**
     * How many threads should be running
     */
    private final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    /**
     * The size of spaces when calculating shot percentages for spaces used by hex maps
     */
    private final int OFFSET = 15;
    /**
     * Save number of new shots added to the database for logging results
     */
    private static int newShots = 0;
    /**
     * A thread safe queue accessible by all threads for retrieving the next search
     */
    private static ConcurrentLinkedQueue<Player> threadSafePlayerQueue = new ConcurrentLinkedQueue<>();

    @Autowired
    private AllTeamAndPlayerScraper allTeamAndPlayerScraper;
    @Autowired
    private DataDoubleChecker dataDoubleChecker;
    @Autowired
    private DatabaseUpdater databaseUpdater;

    private final ResourceBundle READER;


    /**
     * Inject scraper dependencies
     *
     * @param allTeamAndPlayerScraper scraper for team and player data
     * @param dataDoubleChecker       object to compare active database against established database to check data accuracy
     * @param databaseUpdater         group of methods to update various general tables
     */
    @Autowired
    public RunHandler(AllTeamAndPlayerScraper allTeamAndPlayerScraper, DataDoubleChecker dataDoubleChecker, DatabaseUpdater databaseUpdater) throws SQLException {
        //External storage must be attached for logging
        assertTrue(Files.isDirectory(Paths.get("/Volumes/easystore/AllShotScraperV2Logs")));
        this.allTeamAndPlayerScraper = allTeamAndPlayerScraper;
        this.dataDoubleChecker = dataDoubleChecker;
        READER = ResourceBundle.getBundle("application");
    }

    /**
     * Adds new shots to the new shot count for logging purposes
     *
     * @param addedShots the number of new shots scraped
     */
    public static void addToNewShotCount(int addedShots) {
        newShots += addedShots;
    }

    /**
     * Gets the player data from the front of the queue
     *
     * @return hashMap of player info
     */
    public static Player pollQueue() {
        return threadSafePlayerQueue.poll();
    }

    /**
     * Creates a queue of all players and their data for threads to poll
     */
    protected void populateThreadSafeQueueWithPlayers(Connection connPlayers, boolean onlyActivePlayers, boolean currentYearOnly, boolean skipExistingPlayerTables, String schemaAlias) {
        try {
            HashSet<String> existingTableNames = new HashSet<>();
            //If skipping existing tables, find tables that already exist
            if (skipExistingPlayerTables) {
                ResultSet rsTables = connPlayers.getMetaData().getTables(ScraperUtilsInterface.super.getSchemaName(schemaAlias), null, "%", new String[]{"TABLE"});
                while (rsTables.next()) {
                    existingTableNames.add(rsTables.getString(3));
                }
                rsTables.close();
            }
            threadSafePlayerQueue = new ConcurrentLinkedQueue<>();
            //Generate SQL
            StringBuilder sqlSelectBuilder = new StringBuilder("SELECT * FROM player_relevant_data");
            if (onlyActivePlayers && !currentYearOnly) {
                sqlSelectBuilder.append(" WHERE currentlyactive=1");
            } else if (!onlyActivePlayers && currentYearOnly) {
                sqlSelectBuilder.append(" WHERE mostrecentactiveyear='").append(READER.getString("currentYear")).append("'");
            } else if (onlyActivePlayers && currentYearOnly) {
                sqlSelectBuilder.append(" WHERE currentlyactive=1 AND mostrecentactiveyear='").append(READER.getString("currentYear")).append("'");
            }
            ResultSet rsPlayers = connPlayers.prepareStatement(sqlSelectBuilder.toString()).executeQuery();
            while (rsPlayers.next()) {
                //Build table name
                String tableName = rsPlayers.getString("lastname").replaceAll("[^A-Za-z0-9]", "") + "_"
                        + rsPlayers.getString("firstname").replaceAll("[^A-Za-z0-9]", "") + "_"
                        + rsPlayers.getInt("id") + "_individual_data";
                //If not skipping tables, add player
                //If skipping tables and table does not exist, add player
                if (!skipExistingPlayerTables || !existingTableNames.contains(tableName)) {
                    threadSafePlayerQueue.add(new Player(rsPlayers.getInt("id") + "", rsPlayers.getString("lastname"), rsPlayers.getString("firstname"),
                            rsPlayers.getString("currentlyactive"), rsPlayers.getString("firstactiveyear"), rsPlayers.getString("mostrecentactiveyear")));
                    LOGGER.info("Adding " + rsPlayers.getString("firstname") + " " + rsPlayers.getString("lastname") + " to queue");
                }
            }
            rsPlayers.close();
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    /**
     * Runs the desired scrapers/analyzers
     */
    protected void runScraper(String schemaPlayers1Alias, String schemaPlayers2Alias, String schemaShots1Alias, String schemaShots2Alias) throws SQLException {
        //Create connections to databases to be used by single threaded processes
        Connection connPlayersSingleThreaded1 = ScraperUtilsInterface.super.setNewConnection(schemaPlayers1Alias);
        Connection connPlayersSingleThreaded2 = schemaPlayers1Alias.equals(schemaPlayers2Alias) ? connPlayersSingleThreaded1 : ScraperUtilsInterface.super.setNewConnection(schemaPlayers2Alias);
        Connection connShotsSingleThreaded1 = ScraperUtilsInterface.super.setNewConnection(schemaShots1Alias);
        Connection connShotsSingleThreaded2 = schemaShots1Alias.equals(schemaShots2Alias) ? connShotsSingleThreaded1 : ScraperUtilsInterface.super.setNewConnection(schemaShots2Alias);
        try {
            if (getTeamAndPlayerData) {
                allTeamAndPlayerScraper.getTeamAndPlayerData(connPlayersSingleThreaded1, connPlayersSingleThreaded2);
            }
            if (getAllPlayersForFirstTime) {
                populateThreadSafeQueueWithPlayers(connPlayersSingleThreaded1, false, false, true, schemaPlayers1Alias);
                scrapePlayers(schemaPlayers1Alias, schemaPlayers2Alias);
            }
            if (updatePlayersForCurrentYearOnly) {
                populateThreadSafeQueueWithPlayers(connPlayersSingleThreaded1, true, true, false, schemaPlayers1Alias);
                scrapePlayers(schemaPlayers1Alias, schemaPlayers2Alias);
            }
            if (doubleCheckPlayerTables) {
                dataDoubleChecker.comparePlayerTables(dropMismatchedTables, schemaPlayers1Alias, "playertrusted");
            }
            if (makeShotLocationAverages) {
                for (int year = 1996; year <= Integer.parseInt(ScraperUtilsInterface.super.getCurrentYear().substring(0, 4)); year++) {
                    databaseUpdater.createShotLocationAverages(year + "", OFFSET, databaseUpdater.getConnShots1());
//                    databaseUpdater.createShotLocationAverages(year + "", OFFSET, databaseUpdater.getConnShots2());
                }
                databaseUpdater.createShotLocationAverages("", OFFSET, databaseUpdater.getConnShots1());
//                databaseUpdater.createShotLocationAverages("", OFFSET, databaseUpdater.getConnShots2());
            }
            if (makeZoneAverages) {
                for (int year = 1996; year <= Integer.parseInt(ScraperUtilsInterface.super.getCurrentYear().substring(0, 4)); year++) {
                    databaseUpdater.getZonedAverages(year + "", databaseUpdater.getConnShots1());
//                    databaseUpdater.getZonedAverages(year + "", databaseUpdater.getConnShots2());
                }
                databaseUpdater.getZonedAverages("", databaseUpdater.getConnShots1());
//                databaseUpdater.getZonedAverages("", databaseUpdater.getConnShots2());
            }
            if (organizePlayersByYear) {
                databaseUpdater.organizeByYear(databaseUpdater.getConnPlayers1());
//                databaseUpdater.organizeByYear(databaseUpdater.getConnPlayers2());
            }
            if (makeDistanceAverages) {
                for (int year = 1996; year <= Integer.parseInt(ScraperUtilsInterface.super.getCurrentYear().substring(0, 4)); year++) {
                    databaseUpdater.getDistancesAndAvg(year + "", databaseUpdater.getConnShots1());
//                    databaseUpdater.getZonedAverages(year + "", databaseUpdater.getConnShots2());
                }
                databaseUpdater.getDistancesAndAvg("", databaseUpdater.getConnShots1());
                //                databaseUpdater.organizeByYear(databaseUpdater.getConnPlayers2());
            }
            if (makePlayTypeTable) {
                databaseUpdater.createPlayTypeTable(databaseUpdater.getConnShots1());
                //databaseUpdater.createPlayTypeTable( databaseUpdater.getConnShots2());
            }
            if (getAllShotsForFirstTime) {
                populateThreadSafeQueueWithPlayers(connPlayersSingleThreaded1, false, false, false, schemaPlayers1Alias);
                scrapeShots(schemaPlayers1Alias, schemaPlayers2Alias, schemaShots1Alias, schemaShots2Alias, false, "");
                LOGGER.info("Total New Shots Added: " + newShots);
            }
            if (updateShotsForCurrentYear) {
                populateThreadSafeQueueWithPlayers(connPlayersSingleThreaded1, true, true, false, schemaPlayers1Alias);
                scrapeShots(schemaPlayers1Alias, schemaPlayers2Alias, schemaShots1Alias, schemaShots2Alias, true, seasonType);
                LOGGER.info("Total New Shots Added: " + newShots);
            }
            if (doubleCheckShotTables) {
                dataDoubleChecker.compareShotTables(dropMismatchedTables, checkFullShots, schemaPlayers1Alias, schemaShots1Alias, "shottrusted");
            }
            if (dropAllEmptyShotTables) {
                Connection connShots = ScraperUtilsInterface.super.setNewConnection(schemaShots1Alias);
                ResultSet rs = connShots.getMetaData().getTables(ScraperUtilsInterface.super.getSchemaName(schemaShots1Alias), null, "%", null);
                //Get each table title
                int counter = 0;
                String tableTitle, sqlDrop, sqlSelect;
                ResultSet rows;
                while (rs.next()) {
                    tableTitle = rs.getString(3);
                    //For each applicable table
                    if (tableTitle.contains("_Playoffs") || tableTitle.contains("_RegularSeason") || tableTitle.contains("_Preseason") || tableTitle.contains("empty")) {
                        sqlSelect = "SELECT count(*) from " + tableTitle;
                        rows = connShots.prepareStatement(sqlSelect).executeQuery();
                        rows.next();
                        if (rows.getInt("count(*)") == 0) {
                            sqlDrop = "DROP TABLE `" + ScraperUtilsInterface.super.getSchemaName(schemaShots1Alias) + "`.`" + tableTitle + "`";
                            connShots.prepareStatement(sqlDrop).execute();
                            LOGGER.info("Dropped " + tableTitle);
                            counter++;
                        }
                    }
                }
                rs.close();
                LOGGER.info("Final Drop Counter: " + counter);
            }
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
        connPlayersSingleThreaded1.close();
        if (!connPlayersSingleThreaded1.equals(connPlayersSingleThreaded2)) {
            connPlayersSingleThreaded2.close();
        }
        connShotsSingleThreaded1.close();
        if (!connShotsSingleThreaded1.equals(connShotsSingleThreaded2)) {
            connShotsSingleThreaded2.close();
        }
        LOGGER.info("END OF RUN");
    }

    /**
     * Runs the scraper once the application is ready
     *
     * @param applicationReadyEvent
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        try {
//            runScraper("playerlocal", "playerremote", "shotlocal", "shotremote");
            runScraper("playerlocal", "playerlocal", "shotlocal", "shotlocal");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected void addToQueueForTesting(Player player) {
        threadSafePlayerQueue.add(player);
    }

    public static Player peekQueue() {
        return threadSafePlayerQueue.peek();
    }

    protected void scrapePlayers(String schemaPlayers1, String schemaPlayers2) throws InterruptedException {
        final ArrayList<Thread> threads = new ArrayList<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            //Create scraper object and connections for each thread
            Thread thread = new Thread(() -> {
                IndividualPlayerScraper individualPlayerScraper = new IndividualPlayerScraper(schemaPlayers1, schemaPlayers2);
                try {
                    Connection connPlayersEachThread1 = ScraperUtilsInterface.super.setNewConnection(schemaPlayers1);
                    Connection connPlayersEachThread2 = schemaPlayers1.equals(schemaPlayers2) ? connPlayersEachThread1 : ScraperUtilsInterface.super.setNewConnection(schemaPlayers2);
                    try {
                        individualPlayerScraper.getPlayerActiveYears(connPlayersEachThread1, connPlayersEachThread2);
                    } catch (InterruptedException ex) {
                        LOGGER.error(ex.getMessage());
                    }
                    connPlayersEachThread1.close();
                    if (!connPlayersEachThread1.equals(connPlayersEachThread2)) {
                        connPlayersEachThread2.close();
                    }
                } catch (SQLException ex) {
                    LOGGER.error(ex.getMessage());
                }
            });
            threads.add(thread);
            thread.start();
            //Prevent all threads from starting at the same time and sending too many requests in a short time
            Thread.sleep(15000);
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    protected void scrapeShots(String schemaPlayers1, String schemaPlayers2, String schemaShots1, String schemaShots2, boolean onlyCurrentSeason, String currentSeasonType) throws InterruptedException {
        final ArrayList<Thread> threads = new ArrayList<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            Thread thread = new Thread(() -> {
                ShotScraper shotScraper = new ShotScraper(schemaShots1, schemaShots2, schemaPlayers1, schemaPlayers2,
                        new IndividualPlayerScraper(schemaPlayers1, schemaPlayers2));
                try {
                    Connection connPlayersEachThread1 = ScraperUtilsInterface.super.setNewConnection(schemaPlayers1);
                    Connection connPlayersEachThread2 = schemaPlayers1.equals(schemaPlayers2) ? connPlayersEachThread1 : ScraperUtilsInterface.super.setNewConnection(schemaPlayers2);
                    Connection connShotsEachThread1 = ScraperUtilsInterface.super.setNewConnection(schemaShots1);
                    Connection connShotsEachThread2 = schemaShots1.equals(schemaShots2) ? connShotsEachThread1 : ScraperUtilsInterface.super.setNewConnection(schemaShots2);
                    try {
                        shotScraper.getEveryShotWithMainThread(connPlayersEachThread1, connPlayersEachThread2, connShotsEachThread1, connShotsEachThread2, onlyCurrentSeason, currentSeasonType);
                    } catch (Exception ex) {
                        LOGGER.error(ex.getMessage());
                    }
                    connPlayersEachThread1.close();
                    if (!connPlayersEachThread1.equals(connPlayersEachThread2)) {
                        connPlayersEachThread2.close();
                    }
                    connShotsEachThread1.close();
                    if (!connShotsEachThread1.equals(connShotsEachThread2)) {
                        connShotsEachThread2.close();
                    }
                } catch (SQLException ex) {
                    LOGGER.error(ex.getMessage());
                }
            });
            threads.add(thread);
            thread.start();
            Thread.sleep(5000);
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }
}
