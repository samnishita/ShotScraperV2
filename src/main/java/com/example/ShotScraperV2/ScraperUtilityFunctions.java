package com.example.ShotScraperV2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class ScraperUtilityFunctions {
    private ResourceBundle reader = null;
    private String currentYear;
    private Logger LOGGER = LoggerFactory.getLogger(ScraperUtilityFunctions.class);

    public ScraperUtilityFunctions() {
        reader = ResourceBundle.getBundle("application");
        this.currentYear = reader.getString("currentYear");
    }

    public Connection setNewConnection(String schema, String location) throws SQLException {
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

    public String getCurrentYear() {
        return currentYear;
    }

    protected String fetchSpecificURL(String url) throws HttpTimeoutException, InterruptedException, IOException {
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
        LOGGER.info("Fetching " + url);
        HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString());
        LOGGER.debug("Response from " + url+":\n"+response);
        return response.body().toString();
    }

    public String buildYear(String year) {
        int subYear = (Integer.parseInt(year) - 1899) % 100;
        String subYearString = subYear < 10 ? "0" + subYear : "" + subYear;
        return Integer.parseInt(year) + "-" + subYearString;
    }
}
