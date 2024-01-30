import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Accumulators;
import org.bson.Document;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
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

    //methodes for the Team Collection

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

    // effectue une jointure entre les deux col et sort par date
    public void listMatchesWithAggregate() {
        MongoCollection<Document> collection = database.getCollection("Match");
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
            MongoCollection<Document> collection = database.getCollection("Team");

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


    //methodes for the Match collection

    public void insertMatch(Document match) {
        try {
            MongoCollection<Document> collection = database.getCollection("Match");
            collection.insertOne(match);
            System.out.println("Match inserted successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Document findMatchById(String id) {
        try {
            MongoCollection<Document> collection = database.getCollection("Match");
            return collection.find(Filters.eq("_id", new ObjectId(id))).first();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void updateMatch(String matchId, Document updateFields) {
        try {
            MongoCollection<Document> collection = database.getCollection("Match");
            collection.updateOne(Filters.eq("_id", new ObjectId(matchId)), new Document("$set", updateFields));
            System.out.println("Match updated successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteMatch(String matchId) {
        try {
            MongoCollection<Document> collection = database.getCollection("Match");
            collection.deleteOne(Filters.eq("_id", new ObjectId(matchId)));
            System.out.println("Match deleted successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Method that lists all the matches in a specicfic staduim

    public void listMatchesInStadium(String stadiumName) {
        try {
            MongoCollection<Document> collection = database.getCollection("Match");

            // Query to find matches where the 'stadium.name' matches the specified name
            FindIterable<Document> Match = collection.find(new Document("stadium.name", stadiumName));

            for (Document match : Match) {
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
                                .append("birthdate", Date.from(LocalDate.of(1987, 6, 24).atStartOfDay(ZoneId.systemDefault()).toInstant())) 
                                .append("position", new Document("name", "Goalkeeper").append("number", 1)),
                        new Document("name", "Benzema")
                                .append("firstName", "Karim")
                                .append("birthdate", Date.from(LocalDate.of(1987, 6, 24).atStartOfDay(ZoneId.systemDefault()).toInstant()))
                                .append("position", new Document("name", "Forward").append("number", 9))
                ));

        Document barcelona = new Document("name", "FC Barcelona")
                .append("colors", Arrays.asList("Blue", "Red"))
                .append("players", Arrays.asList(
                        new Document("name", "Ter Stegen")
                                .append("firstName", "Marc-André")
                                .append("birthdate", Date.from(LocalDate.of(1987, 6, 24).atStartOfDay(ZoneId.systemDefault()).toInstant())) 
                                .append("position", new Document("name", "Goalkeeper").append("number", 1)),
                        new Document("name", "Messi")
                                .append("firstName", "Lionel")
                                .append("birthdate", Date.from(LocalDate.of(1987, 6, 24).atStartOfDay(ZoneId.systemDefault()).toInstant())) 
                                .append("position", new Document("name", "Forward").append("number", 10))
                ));

        manager.insertTeam(barcelona);

    // creat and insert a new match
        Document newMatch = new Document()
        .append("Team", Arrays.asList(
                new Document("teamId", "65a320c3480a9cddac5cb84d")
                        .append("name", "Raja Casablanca")
                        .append("goals", 3),
                new Document("teamId", "65a3392164a296570cf3229a")
                        .append("name", "Real Madrid")
                        .append("goals", 2)))
        .append("birthdate", Date.from(LocalDate.of(1987, 6, 24).atStartOfDay(ZoneId.systemDefault()).toInstant())) 
        .append("stadium", new Document("name", "Donor")
                .append("city", new Document("name", "Casablanca")
                        .append("country", "Maroc")));

        manager.insertMatch(newMatch);


    // Find a match by ID
        Document foundMatch = manager.findMatchById("65a33ff4480a9cddac5cb86b");
        System.out.println(foundMatch.toJson());

    // Update a match
        Document updateFields = new Document("Donor", "Camp Nou");
        manager.updateMatch("65a33ff4480a9cddac5cb86b", updateFields);

    // Delete a match
        manager.deleteMatch("65b910516a9bc7bcb066851b");
    //group by
        //manager.listMatchesInStadium("Stade de Port Taraport");

    //liste toute les equipes
        //manager.listAllTeams();

    //group teams by color
        //manager.groupTeamsByColor();





        manager.close();
    }
}
