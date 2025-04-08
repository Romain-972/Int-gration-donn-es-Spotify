use spotify_data;

CREATE TABLE Artist (
    artist_uri VARCHAR(255) PRIMARY KEY,
    artist_name VARCHAR(255) NOT NULL
);

CREATE TABLE Albums (
    album_uri VARCHAR(255) PRIMARY KEY,
    album_name VARCHAR(255) NOT NULL
);

CREATE TABLE Tracks (
    track_uri VARCHAR(255) PRIMARY KEY,
    track_name VARCHAR(255) NOT NULL,
    duration_ms INT,
    artist_uri VARCHAR(255),
    album_uri VARCHAR(255),
    FOREIGN KEY (artist_uri) REFERENCES artist(artist_uri),
    FOREIGN KEY (album_uri) REFERENCES albums(album_uri)
);


CREATE TABLE playlist (
    pid VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    collaborative BOOLEAN,
    num_followers INT
);


CREATE TABLE posseder (
    pos INT,
    track_uri VARCHAR(255),
    pid VARCHAR(255),
    PRIMARY KEY (track_uri, pid),
    FOREIGN KEY (track_uri) REFERENCES tracks(track_uri),
    FOREIGN KEY (pid) REFERENCES playlist(pid)
);




