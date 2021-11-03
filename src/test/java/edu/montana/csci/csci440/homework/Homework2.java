package edu.montana.csci.csci440.homework;

import edu.montana.csci.csci440.DBTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Homework2 extends DBTest {

    @Test
    /*
     * Create a view tracksPlus to display the artist, song title, album, and genre for all tracks.
     */
    public void createTracksPlusView(){
        //TODO fill this in
        executeDDL("CREATE VIEW [tracksPlus] AS\n" +
                "    SELECT a2.Name as ArtistName, tracks.TrackId as TrackId, a.Title as AlbumTitle, g.Name as GenreName\n" +
                "    FROM tracks\n" +
                "    JOIN albums a on tracks.AlbumId = a.AlbumId\n" +
                "    JOIN artists a2 on a.ArtistId = a2.ArtistId\n" +
                "    JOIN genres g on tracks.GenreId = g.GenreId");

        List<Map<String, Object>> results = executeSQL("SELECT * FROM tracksPlus ORDER BY TrackId");
        assertEquals(3503, results.size());
        assertEquals("Rock", results.get(0).get("GenreName"));
        assertEquals("AC/DC", results.get(0).get("ArtistName"));
        assertEquals("For Those About To Rock We Salute You", results.get(0).get("AlbumTitle"));
    }

    @Test
    /*
     * Create a table grammy_infos to track grammy information for an artist.  The table should include
     * a reference to the artist, the album (if the grammy was for an album) and the song (if the grammy was
     * for a song).  There should be a string column indicating if the artist was nominated or won.  Finally,
     * there should be a reference to the grammy_category table
     *
     * Create a table grammy_category
     */
    public void createGrammyInfoTable(){
        //TODO fill these in
        executeDDL("CREATE TABLE grammy_categories (Name varchar(255), GrammyCategoryId INTEGER PRIMARY KEY);");
        executeDDL("CREATE TABLE grammy_infos (\n" +
                "    ArtistId integer,\n" +
                "    AlbumId integer,\n" +
                "    TrackId integer,\n" +
                "    Status varchar(255),\n" +
                "    GrammyCategoryId INTEGER PRIMARY KEY\n" +
                ");");

        // TEST CODE
        executeUpdate("INSERT INTO grammy_categories(Name) VALUES ('Greatest Ever');");
        Object categoryId = executeSQL("SELECT GrammyCategoryId FROM grammy_categories").get(0).get("GrammyCategoryId");

        executeUpdate("INSERT INTO grammy_infos(ArtistId, AlbumId, TrackId, GrammyCategoryId, Status) VALUES (1, 1, 1, " + categoryId + ",'Won');");

        List<Map<String, Object>> results = executeSQL("SELECT * FROM grammy_infos");
        assertEquals(1, results.size());
        assertEquals(1, results.get(0).get("ArtistId"));
        assertEquals(1, results.get(0).get("AlbumId"));
        assertEquals(1, results.get(0).get("TrackId"));
        assertEquals(1, results.get(0).get("GrammyCategoryId"));
    }

    @Test
    /*
     * Bulk insert five categories of your choosing in the genres table
     */
    public void bulkInsertGenres(){
        Integer before = (Integer) executeSQL("SELECT COUNT(*) as COUNT FROM genres").get(0).get("COUNT");

        //TODO fill this in
        executeUpdate("INSERT INTO genres (GenreId, Name)\n" +
                "SELECT 26, 'Country'\n" +
                "UNION ALL\n" +
                "SELECT 27, 'Horror'\n" +
                "UNION ALL\n" +
                "SELECT 28, 'Western'\n" +
                "UNION ALL\n" +
                "SELECT 29, 'Movies'\n" +
                "UNION ALL\n" +
                "SELECT 30, 'Sports'");

        Integer after = (Integer) executeSQL("SELECT COUNT(*) as COUNT FROM genres").get(0).get("COUNT");
        assertEquals(before + 5, after);
    }

}
