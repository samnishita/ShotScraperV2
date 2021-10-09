package com.example.ShotScraperV2.objects;

import java.util.Objects;

/**
 * Team object with basic information
 */
public class Team {
    private int id;
    private String abbreviation,casualName,firstName,secondName;

    public Team(int id, String abbreviation, String casualName, String firstName, String secondName) {
        this.id = id;
        this.abbreviation = abbreviation;
        this.casualName = casualName;
        this.firstName = firstName;
        this.secondName = secondName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Team team = (Team) o;
        return id == team.id && Objects.equals(abbreviation, team.abbreviation) && Objects.equals(casualName, team.casualName) && Objects.equals(firstName, team.firstName) && Objects.equals(secondName, team.secondName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, abbreviation, casualName, firstName, secondName);
    }
}
