package edu.montana.csci.csci440.homework;

import edu.montana.csci.csci440.DBTest;
import edu.montana.csci.csci440.model.Track;
import edu.montana.csci.csci440.util.DB;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Homework3 extends DBTest {

    @Test
    /*
     * Use a transaction to safely move milliseconds from one track to anotherls
     *
     * You will need to use the JDBC transaction API, outlined here:
     *
     *   https://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html
     *
     */
    public void useATransactionToSafelyMoveMillisecondsFromOneTrackToAnother() throws SQLException {

        Track track1 = Track.find(1);
        Long track1InitialTime = track1.getMilliseconds();
        Track track2 = Track.find(2);
        Long track2InitialTime = track2.getMilliseconds();

        try(Connection connection = DB.connect()){
            connection.setAutoCommit(false);
            PreparedStatement subtract = connection.prepareStatement("UPDATE tracks SET Milliseconds = Milliseconds - 10 WHERE TrackId=1;");
//            subtract.setLong(1, 1);
//            subtract.setLong(2, 0);
            subtract.execute();

            PreparedStatement add = connection.prepareStatement("UPDATE tracks SET Milliseconds = Milliseconds + 10 WHERE TrackId=2;");
//            subtract.setLong(1, 2); // not catching trackId for some reason
//            subtract.setLong(2, 0);
            add.execute();

            // commit with the connection
            connection.commit();
        }

        // refresh tracks from db
        track1 = Track.find(1);
        track2 = Track.find(2);
        assertEquals(track1.getMilliseconds(), track1InitialTime - 10);
        assertEquals(track2.getMilliseconds(), track2InitialTime + 10);
    }

    @Test
    /*
     * Select tracks that have been sold more than once (> 1)
     *
     * Select the albumbs that have tracks that have been sold more than once (> 1)
     *   NOTE: This is NOT the same as albums whose tracks have been sold more than once!
     *         An album could have had three tracks, each sold once, and should not be included
     *         in this result.  It should only include the albums of the tracks found in the first
     *         query.
     * */
    public void selectPopularTracksAndTheirAlbums() throws SQLException {

        // HINT: join to invoice items and do a group by/having to get the right answer
        List<Map<String, Object>> tracks = executeSQL("SELECT tracks.Name as Name, tracks.TrackId as TrackId, ii.TrackId, count(ii.TrackId) as Count\n" +
                "FROM tracks\n" +
                "LEFT JOIN invoice_items ii on tracks.TrackId = ii.TrackId\n" +
                "GROUP BY tracks.Name, ii.TrackId having count(ii.TrackId) > 1;");
        assertEquals(256, tracks.size());

        // HINT: join to tracks and invoice items and do a group by/having to get the right answer
        //       note: you will need to use the DISTINCT operator to get the right result!
        List<Map<String, Object>> albums = executeSQL("SELECT DISTINCT a.Title as AlbumName\n" +
                "FROM tracks\n" +
                "LEFT JOIN invoice_items ii on tracks.TrackId = ii.TrackId\n" +
                "JOIN albums a on a.AlbumId = tracks.AlbumId\n" +
                "GROUP BY tracks.Name, ii.TrackId having count(ii.TrackId) > 1;SELECT DISTINCT a.Title as AlbumName\n" +
                "FROM tracks\n" +
                "LEFT JOIN invoice_items ii on tracks.TrackId = ii.TrackId\n" +
                "JOIN albums a on a.AlbumId = tracks.AlbumId\n" +
                "GROUP BY tracks.Name, ii.TrackId having count(ii.TrackId) > 1;");
        assertEquals(166, albums.size());
    }

    @Test
    /*
     * Select customers emails who are assigned to Jane Peacock as a Rep and
     * who have purchased something from the 'Rock' Genre
     *
     * Please use an IN clause and a sub-select to generate customer IDs satisfying the criteria
     * */
    public void selectCustomersMeetingCriteria() throws SQLException {
        // HINT: join to invoice items and do a group by/having to get the right answer
        List<Map<String, Object>> tracks = executeSQL("SELECT DISTINCT customers.Email as Email, customers.CustomerId as CustomerId\n" +
                "FROM customers\n" +
                "JOIN invoices i on customers.CustomerId = i.CustomerId\n" +
                "JOIN invoice_items ii on i.InvoiceId = ii.InvoiceId\n" +
                "JOIN tracks t on t.TrackId = ii.TrackId\n" +
                "JOIN genres g on t.GenreId = g.GenreId\n" +
                "WHERE customers.SupportRepId = 3 and g.GenreId in (SELECT tracks.GenreId From tracks WHERE tracks.GenreId=1)" );
        assertEquals(21, tracks.size());
    }


}
