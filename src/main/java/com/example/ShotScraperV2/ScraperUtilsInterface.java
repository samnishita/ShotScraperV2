package com.example.ShotScraperV2;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ResourceBundle;

/**
 * Provides additional functionality
 */
public interface ScraperUtilsInterface {

    /**
     * Establishes connection to an allowed database
     * @param schema The schema name of the database
     * @param location The location of the database
     * @return Returns a Connection to the database
     * @throws SQLException If connection is denied
     */
     default Connection setNewConnection(String schema, String location) throws SQLException {
         ResourceBundle reader= ResourceBundle.getBundle("application");
        if (schema.equals("nbaplayerinfov2") && location.equals("local")) {
            return DriverManager.getConnection(reader.getString("spring.playerlocal.jdbc-url"), reader.getString("spring.playerlocal.username"), reader.getString("spring.playerlocal.password"));
        } else if (schema.equals("nbaplayerinfov2") && location.equals("remote")) {
            return DriverManager.getConnection(reader.getString("spring.playerremote.jdbc-url"), reader.getString("spring.playerremote.username"), reader.getString("spring.playerremote.password"));
        } else if (schema.equals("nbaplayerinfo")) {
            return DriverManager.getConnection(reader.getString("spring.playertrusted.jdbc-url"), reader.getString("spring.playertrusted.username"), reader.getString("spring.playertrusted.password"));
        } else if (schema.equals("nbashotsnew")) {
            return DriverManager.getConnection(reader.getString("spring.shottrusted.jdbc-url"), reader.getString("spring.shottrusted.username"), reader.getString("spring.shottrusted.password"));
        } else if (schema.equals("nbashotsv2") && location.equals("local")) {
            return DriverManager.getConnection(reader.getString("spring.shotlocal.jdbc-url"), reader.getString("spring.shotlocal.username"), reader.getString("spring.shotlocal.password"));
        } else if (schema.equals("nbashotsv2") && location.equals("remote")) {
            return DriverManager.getConnection(reader.getString("spring.shotremote.jdbc-url"), reader.getString("spring.shotremote.username"), reader.getString("spring.shotremote.password"));
        } else {
            throw new IllegalStateException("Error in creating " + this.getClass() + ": No such database exists (schema: " + schema + ", location:" + location + ")");
        }
    }

    /**
     * Reads and returns the current season
     * @return The current season
     */
    default String getCurrentYear() {
        ResourceBundle reader= ResourceBundle.getBundle("application");
        return reader.getString("currentYear");
    }

    /**
     * Builds a season as YYYY-YY from an input year of YYYY because seasons span multiple calendar years
     * @param year Input year as YYYY
     * @return Season as YYYY-YY
     */
    default String buildYear(String year) {
        int subYear = (Integer.parseInt(year) - 1899) % 100;
        String subYearString = subYear < 10 ? "0" + subYear : "" + subYear;
        return Integer.parseInt(year) + "-" + subYearString;
    }

    /**
     * Fetches a URL and returns the response
     * @param url The URL to be fetched
     * @return The response body from the URL as a String
     * @throws HttpTimeoutException If the request times out
     * @throws InterruptedException If the request is interrupted
     * @throws IOException If the request is interrupted
     */
    default String fetchSpecificURL(String url) throws HttpTimeoutException, InterruptedException, IOException {
        Thread.sleep((long)(Math.random()*20000));
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .header("accept", "application/json")
                .header("Origin", "https://www.nba.com")
                .header("Referer", "https://www.nba.com/")
                .header("Access-Control-Request-Headers", "x-nba-stats-origin,x-nba-stats-token")
                .header("Access-Control-Request-Method", "GET")
                .timeout(Duration.ofSeconds(60))
                .build();
        HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body().toString();
    }
}
