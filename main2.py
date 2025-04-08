import os
import json
import mysql.connector
import timeit
from time import time

start_time = time()

db = mysql.connector.connect(
    host="**",
    user="**",
    password="**.",
    database="**"
)

cursor = db.cursor()



cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

tables = ["albums", "artist", "playlist", "posseder", "tracks"]
for table in tables:
    cursor.execute(f"TRUNCATE TABLE {table};")

cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

  
    
querie = {
    "playlist" : "INSERT INTO playlist (pid, name, collaborative, num_followers) VALUES (%s, %s, %s, %s)", 
    "artist" : "INSERT INTO artist (artist_uri, artist_name) VALUES (%s, %s)",
    "albums" : "INSERT INTO albums (album_uri, album_name) VALUES (%s, %s)",
    "tracks" : "INSERT INTO tracks (track_uri, track_name, duration_ms, artist_uri, album_uri) VALUES (%s, %s, %s, %s, %s)",
    "posseder" : "INSERT INTO posseder (pos, track_uri, pid) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE pos = VALUES(pos)"
}
        
        
batch_size = 1000
file_path = r"***"
json_files = [f for f in os.listdir(file_path) if f.endswith('.json')]
json_files = sorted(json_files)[:1000]
max_length = 255



def integration_files() : 
    tracks_uri = set()
    albums_uri = set()
    artists_uri = set()

    for file_name in json_files:
        file_full_path = os.path.join(file_path, file_name)

        time_file_S = timeit.default_timer()

        with open(file_full_path, encoding='utf-8') as f:
            data = json.load(f)
            playlists = data.get("playlists", [])

            
            playlist_data, artist_data, album_data, track_data, posseder_data = set(), set(), set(), set(), set()
        
        
            for playlist in playlists : 
                playlist_data.add((playlist["pid"], playlist["name"], playlist["collaborative"], playlist["num_followers"]))

                for track in playlist["tracks"] :
                    
                    if track["artist_uri"] not in artists_uri:
                        artist_data.add((track["artist_uri"], track["artist_name"][:max_length]))
                        artists_uri.add(track["artist_uri"])
                    else :
                        continue
                    
                    if track["album_uri"] not in albums_uri:
                        album_data.add((track["album_uri"], track["album_name"][:max_length]))
                        albums_uri.add(track["album_uri"])
                    else :
                        continue 

                    if track["track_uri"] not in tracks_uri:
                        track_data.add((track["track_uri"], track["track_name"], track["duration_ms"], track["artist_uri"], track["album_uri"]))
                        tracks_uri.add(track["track_uri"])
                    else :
                        continue

                    posseder_data.add((track["pos"], track["track_uri"], playlist["pid"]))
        

            for table, data in [("playlist", list(playlist_data)), ("artist", list(artist_data)), ("albums", list(album_data)), ("tracks", list(track_data)), ("posseder", list(posseder_data))]:
                    if data:
                        for i in range(0, len(data), batch_size):
                            batch = data[i:i + batch_size]
                            cursor.executemany(querie[table], batch)
                            
            

        time_file = timeit.default_timer() - time_file_S
        print(f"Le fichier {file_name} a pris {time_file:.2f} seconds a etre traité")


integration_files()


db.commit()
cursor.close()
db.close()

end_time = time()

execution_time = end_time - start_time
print(f"Le script a été exécuté en {execution_time:.2f} secondes.")











