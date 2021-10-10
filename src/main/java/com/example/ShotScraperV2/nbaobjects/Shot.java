package com.example.ShotScraperV2.nbaobjects;

import java.util.Objects;

public class Shot {
    private String uniqueShotId, lastName, firstName, season, seasonType, date, time, shotType, playType, teamName, awayTeamName, homeTeamName, shotZoneBasic, shotZoneArea, shotZoneRange;
    private int playerId, gameId, gameEventId, minutes, seconds, x, y, distance, make, period, teamId, awayTeamId, homeTeamId, atHome;

    public Shot(String lastName, String firstName, String season, String seasonType, String date, String time, String shotType, String playType, String teamName, String awayTeamName, String homeTeamName, String shotZoneBasic, String shotZoneArea, String shotZoneRange, int playerId, int gameId, int gameEventId, int minutes, int seconds, int x, int y, int distance, int make, int period, int teamId, int awayTeamId, int homeTeamId, int atHome) {
        this.uniqueShotId = playerId+"-"+gameId+"-"+gameEventId;
        this.lastName = lastName;
        this.firstName = firstName;
        this.season = season;
        this.seasonType = seasonType;
        this.date = date;
        this.time = time;
        this.shotType = shotType;
        this.playType = playType;
        this.teamName = teamName;
        this.awayTeamName = awayTeamName;
        this.homeTeamName = homeTeamName;
        this.shotZoneBasic = shotZoneBasic;
        this.shotZoneArea = shotZoneArea;
        this.shotZoneRange = shotZoneRange;
        this.playerId = playerId;
        this.gameId = gameId;
        this.gameEventId = gameEventId;
        this.minutes = minutes;
        this.seconds = seconds;
        this.x = x;
        this.y = y;
        this.distance = distance;
        this.make = make;
        this.period = period;
        this.teamId = teamId;
        this.awayTeamId = awayTeamId;
        this.homeTeamId = homeTeamId;
        this.atHome = atHome;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Shot shot = (Shot) o;
        return playerId == shot.playerId && gameId == shot.gameId && gameEventId == shot.gameEventId && minutes == shot.minutes && seconds == shot.seconds && x == shot.x && y == shot.y && distance == shot.distance && make == shot.make && period == shot.period && teamId == shot.teamId && awayTeamId == shot.awayTeamId && homeTeamId == shot.homeTeamId && atHome == shot.atHome && uniqueShotId.equals(shot.uniqueShotId) && Objects.equals(lastName, shot.lastName) && Objects.equals(firstName, shot.firstName) && season.equals(shot.season) && seasonType.equals(shot.seasonType) && Objects.equals(date, shot.date) && Objects.equals(time, shot.time) && Objects.equals(shotType, shot.shotType) && Objects.equals(playType, shot.playType) && Objects.equals(teamName, shot.teamName) && Objects.equals(awayTeamName, shot.awayTeamName) && Objects.equals(homeTeamName, shot.homeTeamName) && Objects.equals(shotZoneBasic, shot.shotZoneBasic) && Objects.equals(shotZoneArea, shot.shotZoneArea) && Objects.equals(shotZoneRange, shot.shotZoneRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueShotId, lastName, firstName, season, seasonType, date, time, shotType, playType, teamName, awayTeamName, homeTeamName, shotZoneBasic, shotZoneArea, shotZoneRange, playerId, gameId, gameEventId, minutes, seconds, x, y, distance, make, period, teamId, awayTeamId, homeTeamId, atHome);
    }
}
