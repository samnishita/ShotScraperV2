package com.example.ShotScraperV2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Component
public class RunHandler {
    private Logger LOGGER = LoggerFactory.getLogger(RunHandler.class);
    //true
    //false
    private final boolean GET_TEAM_AND_PLAYER_DATA = false;
    private final boolean GET_ALL_PLAYERS = false;
    private final boolean ONLY_ACTIVE_PLAYERS = false;
    private final boolean SHOULD_RERUN = false;
    private final boolean UPDATE_PLAYERS_FOR_CURRENT_YEAR_ONLY = false;
    private final boolean ORGANIZE_BY_YEAR = false;
    //
    private final boolean GET_ALL_SHOTS = false;
    private final boolean UPDATE_SHOTS_FOR_CURRENT_YEAR = false;
    private final boolean MAKE_SHOT_LOCATION_AVERAGES = false;
    private final boolean MAKE_ZONE_AVERAGES = false;
    private final boolean MAKE_DISTANCE_AVERAGES = false;
    private final boolean MAKE_PLAY_TYPE_TABLE = false;
    private final boolean DROP_ALL_EMPTY_SHOT_TABLES = false;
    //
    private final boolean DOUBLE_CHECK_PLAYER_TABLES = false;
    private final boolean DOUBLE_CHECK_SHOT_TABLES = true;
    private final boolean DROP_MISMATCHED_TABLES = false;
    private String seasonType = "reg";//preseason,playoffs
    private final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private final int OFFSET = 15;

    private static int newShots = 0;
    private static ConcurrentLinkedQueue<HashMap<String, String>> threadSafeMapColumnsToValues = new ConcurrentLinkedQueue<>();


    private ScraperUtilityFunctions scraperUtilityFunctions = new ScraperUtilityFunctions();
    @Autowired
    private AllTeamAndPlayerScraper allTeamAndPlayerScraper;
    @Autowired
    private DataDoubleChecker dataDoubleChecker;
    @Autowired
    private DatabaseUpdater databaseUpdater;

    @Autowired
    public RunHandler(AllTeamAndPlayerScraper allTeamAndPlayerScraper, DataDoubleChecker dataDoubleChecker, DatabaseUpdater databaseUpdater) {
        assertTrue(Files.isDirectory(Paths.get("/Volumes/easystore/AllShotScraperV2Logs")));
        this.allTeamAndPlayerScraper = allTeamAndPlayerScraper;
        this.dataDoubleChecker = dataDoubleChecker;
        final ResourceBundle READER = ResourceBundle.getBundle("application");
        try {
            if (GET_TEAM_AND_PLAYER_DATA) {
                allTeamAndPlayerScraper.getTeamAndPlayerData();
            }
            if (GET_ALL_PLAYERS) {
                populateThreadSafeQueueWithPlayers();
                final ArrayList<Thread> threads = new ArrayList<>();
                for (int i = 0; i < THREAD_COUNT; i++) {
                    Thread thread = new Thread(() -> {
                        IndividualPlayerScraper individualPlayerScraper = new IndividualPlayerScraper(READER.getString("playerschema1"),
                                READER.getString("playerlocation1"), READER.getString("playerschema2"), READER.getString("playerlocation2"));
                        individualPlayerScraper.getAllActiveYearsUsingMain(SHOULD_RERUN);
                    });
                    threads.add(thread);
                    thread.start();
                    Thread.sleep(15000);
                }
                for (Thread thread : threads) {
                    thread.join();
                }
            }
            if (UPDATE_PLAYERS_FOR_CURRENT_YEAR_ONLY) {
                populateThreadSafeQueueWithPlayers();
                final ArrayList<Thread> threads = new ArrayList<>();
                for (int i = 0; i < THREAD_COUNT; i++) {
                    Thread thread = new Thread(() -> {
                        IndividualPlayerScraper individualPlayerScraper = new IndividualPlayerScraper(READER.getString("playerschema1"),
                                READER.getString("playerlocation1"), READER.getString("playerschema2"), READER.getString("playerlocation2"));
                        individualPlayerScraper.updateForCurrentYear(ONLY_ACTIVE_PLAYERS, seasonType);
                    });
                    threads.add(thread);
                    thread.start();
                    Thread.sleep(15000);
                }
                for (Thread thread : threads) {
                    thread.join();
                }
            }
            if (DOUBLE_CHECK_PLAYER_TABLES) {
                dataDoubleChecker.comparePlayerTables(DROP_MISMATCHED_TABLES);
            }
            if (MAKE_SHOT_LOCATION_AVERAGES) {
                for (int year = 1996; year <= Integer.parseInt(scraperUtilityFunctions.getCurrentYear().substring(0, 4)); year++) {
                    databaseUpdater.createShotLocationAverages(year + "", OFFSET, databaseUpdater.getConnShots1());
//                    databaseUpdater.createShotLocationAverages(year + "", OFFSET, databaseUpdater.getConnShots2());
                }
                databaseUpdater.createShotLocationAverages("", OFFSET, databaseUpdater.getConnShots1());
//                databaseUpdater.createShotLocationAverages("", OFFSET, databaseUpdater.getConnShots2());
            }
            if (MAKE_ZONE_AVERAGES) {
                for (int year = 1996; year <= Integer.parseInt(scraperUtilityFunctions.getCurrentYear().substring(0, 4)); year++) {
                    databaseUpdater.getZonedAverages(year + "", databaseUpdater.getConnShots1());
//                    databaseUpdater.getZonedAverages(year + "", databaseUpdater.getConnShots2());
                }
                databaseUpdater.getZonedAverages("", databaseUpdater.getConnShots1());
//                databaseUpdater.getZonedAverages("", databaseUpdater.getConnShots2());
            }
            if (ORGANIZE_BY_YEAR) {
                databaseUpdater.organizeByYear(databaseUpdater.getConnPlayers1());
//                databaseUpdater.organizeByYear(databaseUpdater.getConnPlayers2());
            }
            if (MAKE_DISTANCE_AVERAGES) {
                for (int year = 1996; year <= Integer.parseInt(scraperUtilityFunctions.getCurrentYear().substring(0, 4)); year++) {
                    databaseUpdater.getDistancesAndAvg(year + "", databaseUpdater.getConnShots1());
//                    databaseUpdater.getZonedAverages(year + "", databaseUpdater.getConnShots2());
                }
                databaseUpdater.getDistancesAndAvg("", databaseUpdater.getConnShots1());
                //                databaseUpdater.organizeByYear(databaseUpdater.getConnPlayers2());
            }
            if (MAKE_PLAY_TYPE_TABLE) {
                databaseUpdater.createPlayTypeTable(databaseUpdater.getConnShots1());
                //databaseUpdater.createPlayTypeTable( databaseUpdater.getConnShots2());
            }
            if (GET_ALL_SHOTS) {
                try {
                    populateThreadSafeQueueWithPlayers();
                    final ArrayList<Thread> threads = new ArrayList<>();
                    for (int i = 0; i < THREAD_COUNT; i++) {
                        Thread thread = new Thread(() -> {
                            ShotScraper shotScraper = new ShotScraper(
                                    READER.getString("shotschema1"), READER.getString("shotlocation1"), READER.getString("shotschema2"), READER.getString("shotlocation2"),
                                    READER.getString("playerschema1"), READER.getString("playerlocation1"), READER.getString("playerschema2"), READER.getString("playerlocation2"));
                            shotScraper.getEveryShotWithMainThread();
                        });
                        threads.add(thread);
                        thread.start();
                        Thread.sleep(5000);
                    }
                    for (Thread thread : threads) {
                        thread.join();
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                LOGGER.info("Total New Shots Added: " + newShots);
            }
            if (DOUBLE_CHECK_SHOT_TABLES) {
                dataDoubleChecker.compareShotTables(DROP_MISMATCHED_TABLES);
            }
            if (UPDATE_SHOTS_FOR_CURRENT_YEAR) {
                populateThreadSafeQueueWithPlayers();
                final ArrayList<Thread> threads = new ArrayList<>();
                for (int i = 0; i < THREAD_COUNT; i++) {
                    Thread thread = new Thread(() -> {
                        ShotScraper shotScraper = new ShotScraper(
                                READER.getString("shotschema1"), READER.getString("shotlocation1"), READER.getString("shotschema2"), READER.getString("shotlocation2"),
                                READER.getString("playerschema1"), READER.getString("playerlocation1"), READER.getString("playerschema2"), READER.getString("playerlocation2"));
                        shotScraper.updateShotsForCurrentYear(seasonType);
                    });
                    threads.add(thread);
                    thread.start();
                    Thread.sleep(15000);
                }
                for (Thread thread : threads) {
                    thread.join();
                }
                LOGGER.info("Total New Shots Added: " + newShots);
            }
            if (DROP_ALL_EMPTY_SHOT_TABLES) {
                Connection connShots = scraperUtilityFunctions.setNewConnection(READER.getString("shotschema1"), READER.getString("shotlocation1"));
                ResultSet rs = connShots.getMetaData().getTables(READER.getString("shotschema1"), null, "%", null);
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
                            sqlDrop = "DROP TABLE `" + READER.getString("shotschema1") + "`.`" + tableTitle + "`";
                            connShots.prepareStatement(sqlDrop).execute();
                            LOGGER.info("Dropped " + tableTitle);
                            counter++;
                        }
                    }
                }
                LOGGER.info("Final Drop Counter: " + counter);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        LOGGER.info("END OF RUN");
        System.exit(0);
    }

    public static void addToNewShotCount(int addedShots) {
        newShots += addedShots;
    }

    public static HashMap<String, String> popConcurrentHashMap() {
        return threadSafeMapColumnsToValues.poll();
    }

    private void populateThreadSafeQueueWithPlayers() {
        try {
            final ResourceBundle READER = ResourceBundle.getBundle("application");
            threadSafeMapColumnsToValues = new ConcurrentLinkedQueue();
            Connection connPlayers1 = scraperUtilityFunctions.setNewConnection(READER.getString("playerschema1"), READER.getString("playerlocation1"));
            String sqlSelect = "SELECT * FROM player_relevant_data";
            if (ONLY_ACTIVE_PLAYERS) {
                sqlSelect = sqlSelect + " WHERE currentlyactive=1";
            }
            ResultSet rsPlayers = connPlayers1.prepareStatement(sqlSelect).executeQuery();
            HashMap<String, String> eachPlayerHashMap;
            while (rsPlayers.next()) {
                eachPlayerHashMap = new HashMap<>();
                eachPlayerHashMap.put("playerID", rsPlayers.getInt("id") + "");
                eachPlayerHashMap.put("lastNameOrig", rsPlayers.getString("lastname"));
                eachPlayerHashMap.put("firstNameOrig", rsPlayers.getString("firstname"));
                eachPlayerHashMap.put("firstactiveyear", rsPlayers.getString("firstactiveyear"));
                eachPlayerHashMap.put("mostrecentactiveyear", rsPlayers.getString("mostrecentactiveyear"));
                eachPlayerHashMap.put("currentlyactive", rsPlayers.getString("currentlyactive"));
                threadSafeMapColumnsToValues.add(eachPlayerHashMap);
            }
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }

    }

}
