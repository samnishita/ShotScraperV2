package com.example.ShotScraperV2;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.MissingResourceException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@DisplayName("ScraperUtilsInterface")
public class ScraperUtilsInterfaceTests implements ScraperUtilsInterface {

    /**
     * Checks that a connection can be established to every database
     *
     * @param schema database schema
     * @throws SQLException If the connection is denied
     */
    @ParameterizedTest
    @ValueSource(strings = {"playerlocal", "playerremote", "playertrusted", "shottrusted", "shotlocal", "shotremote", "playertest", "shottest"})
    @DisplayName("can connect to any database")
    void shouldConnectToAnyDatabase(String schema) throws SQLException {
        Connection conn = ScraperUtilsInterface.super.setNewConnection(schema);
        assertNotNull(conn);
        conn.close();
    }

    /**
     * Checks that a bad database schema provided to the method will throw an exception
     */
    @Test
    @DisplayName("throws an exception when trying to connect to a nonexistent schema")
    void shouldThrowExceptionForBadDatabaseSchema() {
        assertThrows(MissingResourceException.class, () -> ScraperUtilsInterface.super.setNewConnection("badschema"));
    }

    /**
     * Tests creating a year output of YYYY-YY from an input of YY
     *
     * @param yearInput four digit String of the year
     * @param expected  seven character String that should be returned
     */
    @ParameterizedTest
    @MethodSource("provideFourDigitYearsForBuildYear")
    @DisplayName("builds seven-character year display from four-character year input")
    void shouldCreateYearPresentation(String yearInput, String expected) {
        assertEquals(expected, ScraperUtilsInterface.super.buildYear(yearInput));
    }

    /**
     * Provides stream of multiple years and the desired results when passed to shouldCreateYearPresentation
     *
     * @return stream of input years and the desired output years
     */
    private static Stream<Arguments> provideFourDigitYearsForBuildYear() {
        return Stream.of(
                Arguments.of("1996", "1996-97"),
                Arguments.of("1999", "1999-00"),
                Arguments.of("2000", "2000-01"),
                Arguments.of("2009", "2009-10"));
    }

}
