import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import org.bson.Document;
import java.util.Arrays;
import java.util.Date;
import org.bson.types.ObjectId;

public class FootballLeagueManager {

    private final MongoClient mongoClient;
    private final MongoDatabase database;

    public FootballLeagueManager(String connectionString, String dbName) {
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(dbName);
    }

    //First we define methodes for the Team Collection

    public void insertTeam(Document team) {
        MongoCollection<Document> collection = database.getCollection("Team");
        collection.insertOne(team);
    }


    public Document findTeamById(String id) {
        MongoCollection<Document> collection = database.getCollection("Team");
        return collection.find(Filters.eq("_id", id)).first();
    }

    public void updateTeamName(String id, String newName) {
        MongoCollection<Document> collection = database.getCollection("Team");
        collection.updateOne(Filters.eq("_id", id), new Document("$set", new Document("name", newName)));
    }

    public void deleteTeam(String id) {
        MongoCollection<Document> collection = database.getCollection("Team");
        collection.deleteOne(Filters.eq("_id", id));
    }

    public void listAllTeams() {
        MongoCollection<Document> collection = database.getCollection("Team");
        FindIterable<Document> Team = collection.find();
        for (Document team : Team) {
            System.out.println(team.toJson());
        }
    }

    public void listMatchesWithAggregate() {
        MongoCollection<Document> collection = database.getCollection("matches");
        AggregateIterable<Document> matchDetails = collection.aggregate(
                Arrays.asList(
                        Aggregates.lookup("teams", "teamId", "_id", "team_info"),
                        Aggregates.sort(new Document("date", 1))
                )
        );

        for (Document match : matchDetails) {
            System.out.println(match.toJson());
        }
    }

    // Group teams by color method
    public void groupTeamsByColor() {
        try {
            MongoCollection<Document> collection = database.getCollection("teams");

            AggregateIterable<Document> groupedResult = collection.aggregate(
                    Arrays.asList(
                            Aggregates.group("$colors", Accumulators.sum("count", 1)),
                            Aggregates.sort(new Document("count", -1)) // Sorting groups by count in descending order
                    )
            );

            for (Document doc : groupedResult) {
                System.out.println(doc.toJson());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Sort teams by foundation year
    public void sortTeamsByFoundationYear() {
        try {
            MongoCollection<Document> collection = database.getCollection("teams");

            FindIterable<Document> sortedTeams = collection.find().sort(new Document("founded", 1)); // 1 for ascending order

            for (Document team : sortedTeams) {
                System.out.println(team.toJson());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Now we define methodes for the Match collection

    public void insertMatch(Document match) {
        try {
            MongoCollection<Document> collection = database.getCollection("matches");
            collection.insertOne(match);
            System.out.println("Match inserted successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Document findMatchById(String id) {
        try {
            MongoCollection<Document> collection = database.getCollection("matches");
            return collection.find(Filters.eq("_id", new ObjectId(id))).first();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void updateMatch(String matchId, Document updateFields) {
        try {
            MongoCollection<Document> collection = database.getCollection("matches");
            collection.updateOne(Filters.eq("_id", new ObjectId(matchId)), new Document("$set", updateFields));
            System.out.println("Match updated successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteMatch(String matchId) {
        try {
            MongoCollection<Document> collection = database.getCollection("matches");
            collection.deleteOne(Filters.eq("_id", new ObjectId(matchId)));
            System.out.println("Match deleted successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Method that lists all the matches in a specicfic staduim

    public void listMatchesInStadium(String stadiumName) {
        try {
            MongoCollection<Document> collection = database.getCollection("matches");

            // Query to find matches where the 'stadium.name' matches the specified name
            FindIterable<Document> matches = collection.find(new Document("stadium.name", stadiumName));

            for (Document match : matches) {
                System.out.println(match.toJson());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }







    public void close() {
        mongoClient.close();
    }

    public static void main(String[] args) {
        FootballLeagueManager manager = new FootballLeagueManager("mongodb://localhost:27017", "leaguenar");

        // Here we call manager methods to interact with the database
        Document realMadrid = new Document("name", "Real Madrid")
                .append("colors", Arrays.asList("White", "Purple"))
                .append("players", Arrays.asList(
                        new Document("name", "Courtois")
                                .append("firstName", "Thibaut")
                                .append("birthdate", new Date(92, 4, 11)) // Date format is (year - 1900, month, day)
                                .append("position", new Document("name", "Goalkeeper").append("number", 1)),
                        new Document("name", "Benzema")
                                .append("firstName", "Karim")
                                .append("birthdate", new Date(87, 11, 19)) // Date format is (year - 1900, month, day)
                                .append("position", new Document("name", "Forward").append("number", 9))
                ));

        manager.insertTeam(realMadrid);

        // Insert a new match
        Document newMatch = new Document("Team", Arrays.asList(new Document("goals", 3), new Document("goals", 2)))
                .append("date", new Date()) 
                .append("stadium", new Document("name", "Donor")
                        .append("city", new Document("name", "Casablanca").append("country", "Maroc")));
        manager.insertMatch(newMatch);

    // Find a match by ID
        Document foundMatch = manager.findMatchById("matchIdHere");
        System.out.println(foundMatch.toJson());

    // Update a match
        Document updateFields = new Document("stadium.name", "New Stadium Name");
        manager.updateMatch("matchIdToUpdate", updateFields);

    // Delete a match
        manager.deleteMatch("matchIdToDelete");



        // Don't forget to close the connection when you're done
        manager.close();
    }
}
