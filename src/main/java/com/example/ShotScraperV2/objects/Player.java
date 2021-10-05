package com.example.ShotScraperV2.objects;

import java.util.*;

/**
 * Player object with basic information
 */
public class Player {

    private int playerId, currentlyActive;
    private String firstName, lastName, thirdName, firstActiveYear, mostRecentActiveYear;
    /**
     * HashMap with (K,V) pairs of (year(YYYY-YY), [preseason activity, regular season activity, playoff activity])
     */
    private HashMap<String, ArrayList<Integer>> eachYearActivityMap;

    /**
     * Constructor for players with two names
     *
     * @param playerId             player's ID
     * @param lastName             player's last name
     * @param firstName            player's first name
     * @param currentlyActive      player's current activity status
     * @param firstActiveYear      player's first active year
     * @param mostRecentActiveYear player's latest active year
     */
    public Player(String playerId, String lastName, String firstName, String currentlyActive, String firstActiveYear, String mostRecentActiveYear) {
        this.playerId = Integer.parseInt(playerId);
        this.lastName = lastName;
        this.firstName = firstName;
        this.firstActiveYear = firstActiveYear;
        this.mostRecentActiveYear = mostRecentActiveYear;
        this.currentlyActive = Integer.parseInt(currentlyActive);
        this.eachYearActivityMap = new HashMap<>();
    }

    /**
     * Constructor for players with only a single name
     *
     * @param playerId             player's ID
     * @param lastName             player's last name
     * @param currentlyActive      player's current activity status
     * @param firstActiveYear      player's first active year
     * @param mostRecentActiveYear player's latest active year
     */
    public Player(String playerId, String lastName, String currentlyActive, String firstActiveYear, String mostRecentActiveYear) {
        this.playerId = Integer.parseInt(playerId);
        this.lastName = lastName;
        this.firstName = "";
        this.firstActiveYear = firstActiveYear;
        this.mostRecentActiveYear = mostRecentActiveYear;
        this.currentlyActive = Integer.parseInt(currentlyActive);
        this.eachYearActivityMap = new HashMap<>();
    }

    /**
     * Constructor for players with three names
     *
     * @param playerId             player's ID
     * @param lastName             player's last name
     * @param firstName            player's first name
     * @param thirdName            player's other name
     * @param currentlyActive      player's current activity status
     * @param firstActiveYear      player's first active year
     * @param mostRecentActiveYear player's latest active year
     */
    public Player(String playerId, String lastName, String firstName, String thirdName, String currentlyActive, String firstActiveYear, String mostRecentActiveYear) {
        this.playerId = Integer.parseInt(playerId);
        this.lastName = lastName;
        this.firstName = firstName;
        this.thirdName = thirdName;
        this.firstActiveYear = firstActiveYear;
        this.mostRecentActiveYear = mostRecentActiveYear;
        this.currentlyActive = Integer.parseInt(currentlyActive);
        this.eachYearActivityMap = new HashMap<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Player player = (Player) o;
        return playerId == player.playerId && currentlyActive == player.currentlyActive && Objects.equals(firstName, player.firstName) && lastName.equals(player.lastName) && firstActiveYear.equals(player.firstActiveYear) && mostRecentActiveYear.equals(player.mostRecentActiveYear);
    }

    @Override
    public int hashCode() {
        return Objects.hash(playerId, currentlyActive, firstName, lastName, firstActiveYear, mostRecentActiveYear);
    }

    /**
     * Add an active year with active season types to the player
     *
     * @param year                  active year
     * @param preseasonActivity     player activity during the preseason (1 = active, 0 = inactive)
     * @param regularSeasonActivity player activity during the regular season (1 = active, 0 = inactive)
     * @param playoffsActivity      player activity during the playoffs (1 = active, 0 = inactive)
     */
    public void addYearActivity(String year, int preseasonActivity, int regularSeasonActivity, int playoffsActivity) {
        Set<Integer> accepted = new HashSet<>(Set.of(0, 1));
        if (accepted.contains(preseasonActivity) && accepted.contains(regularSeasonActivity) && accepted.contains(playoffsActivity)) {
            this.eachYearActivityMap.put(year, new ArrayList<>(Arrays.asList(preseasonActivity, regularSeasonActivity, playoffsActivity)));
        }
    }

    /**
     * Generates an array of player details in the same order as it was originally scraped
     *
     * @return array of player details as Strings
     */
    public String[] generatePlayerDetailsArray() {
        return new String[]{this.playerId + "", this.lastName, this.firstName, this.currentlyActive + "", this.firstActiveYear, this.mostRecentActiveYear};
    }
}
