import pymongo
from faker import Faker
from random import choice, randint
from datetime import datetime, timedelta
import random
import string

# Connexion à MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['leaguenar']
teams = db['Team']
matches = db['Match']


# gener une date ransom
def random_date():
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2024, 12, 31)
    return start_date + (end_date - start_date) * random.random()

""""
def random_name(length=6):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def random_birthdate():
    return datetime(randint(1980, 2000), randint(1, 12), randint(1, 28))

def random_color():
    colors = ['Red', 'Blue', 'Green', 'Yellow', 'Purple', 'Orange', 'Black', 'White']
    return random.choice(colors)
"""

#instance de faker
fake = Faker()

# methode pour generer un jouer random

def generate_player():
    return {
        "name": fake.last_name(),
        "firstName": fake.first_name(),
        "birthdate": {"$date": random_date().isoformat() + 'Z'},
        "position": {
            "name": choice(["Goalkeeper", "Defender", "Midfielder", "Forward"]),
            "number": randint(1, 99)
        }
    }

# Générer des données pour Teams
team_data = []
for _ in range(1000):  # nb equipe
    team_name = 'FC ' + fake.city()
    players = [generate_player() for _ in range(randint(11, 25))] #nb joueurs
    team = {
        "name": team_name,
        "colors": [fake.color_name(), fake.color_name()],
        "players": players
    }
    team_data.append(team)
teams.insert_many(team_data)

# Générer des données pour Matches
team_ids = [team['_id'] for team in teams.find()]
for _ in range(1000):  # Nombre de matches à générer
    city = fake.city()
    match_teams = random.sample(team_ids, 2)
    match = {
        "teams": [
            {"teamId": str(match_teams[0]), "name": teams.find_one({'_id': match_teams[0]})['name'], "goals": randint(0, 5)},
            {"teamId": str(match_teams[1]), "name": teams.find_one({'_id': match_teams[1]})['name'], "goals": randint(0, 5)}
        ],
        "date": {"$date": random_date().isoformat() + 'Z'},
        "stadium": {
            "name": 'Stade de ' + city,
            "city": {
                "name": city,
                "country": fake.country()
            }
        }
    }
    matches.insert_one(match)
