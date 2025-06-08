-- DiscoStar Database Schema
-- SQLite schema for Discogs data and user collections

-- Core Discogs Entity Tables

CREATE TABLE artists (
    id INTEGER PRIMARY KEY,  -- Discogs artist ID
    name TEXT NOT NULL,
    real_name TEXT,
    profile TEXT,
    data_quality TEXT,
    name_variations TEXT,  -- JSON array
    aliases TEXT,          -- JSON array  
    urls TEXT,            -- JSON array
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE labels (
    id INTEGER PRIMARY KEY,  -- Discogs label ID
    name TEXT NOT NULL,
    contact_info TEXT,
    profile TEXT,
    data_quality TEXT,
    urls TEXT,            -- JSON array
    parent_label INTEGER,
    sublabels TEXT,       -- JSON array of IDs
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parent_label) REFERENCES labels(id)
);

CREATE TABLE masters (
    id INTEGER PRIMARY KEY,  -- Discogs master ID
    title TEXT NOT NULL,
    main_release INTEGER,   -- ID of main release
    year INTEGER,
    data_quality TEXT,
    notes TEXT,
    genres TEXT,           -- JSON array
    styles TEXT,           -- JSON array
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE releases (
    id INTEGER PRIMARY KEY,  -- Discogs release ID
    master_id INTEGER,
    title TEXT NOT NULL,
    year INTEGER,
    country TEXT,
    released DATE,
    notes TEXT,
    data_quality TEXT,
    status TEXT,           -- Accepted, Draft, etc.
    genres TEXT,           -- JSON array
    styles TEXT,           -- JSON array
    formats TEXT,          -- JSON array with format details
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (master_id) REFERENCES masters(id)
);

-- Relationship Tables

CREATE TABLE release_artists (
    release_id INTEGER,
    artist_id INTEGER,
    name TEXT,             -- Artist name as credited
    anv TEXT,              -- Artist name variation
    join_relation TEXT,    -- "&", "feat.", etc.
    role TEXT,             -- "vocals", "guitar", etc.
    tracks TEXT,           -- Track numbers where artist appears
    PRIMARY KEY (release_id, artist_id, role),
    FOREIGN KEY (release_id) REFERENCES releases(id),
    FOREIGN KEY (artist_id) REFERENCES artists(id)
);

CREATE TABLE release_labels (
    release_id INTEGER,
    label_id INTEGER,
    catalog_number TEXT,
    PRIMARY KEY (release_id, label_id, catalog_number),
    FOREIGN KEY (release_id) REFERENCES releases(id),
    FOREIGN KEY (label_id) REFERENCES labels(id)
);

CREATE TABLE tracks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    release_id INTEGER NOT NULL,
    position TEXT,         -- "A1", "1", "B2", etc.
    title TEXT NOT NULL,
    duration TEXT,         -- "3:45"
    type TEXT,             -- "track", "index", "heading"
    FOREIGN KEY (release_id) REFERENCES releases(id)
);

-- User Collection Tables

CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    discogs_username TEXT UNIQUE NOT NULL,
    discogs_user_id INTEGER UNIQUE,
    display_name TEXT,
    profile_url TEXT,
    avatar_url TEXT,
    last_sync DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE user_collection (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    release_id INTEGER NOT NULL,
    folder_id INTEGER DEFAULT 1,  -- Discogs folder ID
    instance_id INTEGER,          -- Discogs collection instance ID
    rating INTEGER CHECK (rating BETWEEN 1 AND 5),
    notes TEXT,
    date_added DATETIME,
    basic_information TEXT,       -- JSON with cached release info
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, release_id, instance_id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (release_id) REFERENCES releases(id)
);

CREATE TABLE collection_folders (
    id INTEGER PRIMARY KEY,       -- Discogs folder ID
    user_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    count INTEGER DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Analytics and Metadata Tables

CREATE TABLE data_sources (
    table_name TEXT,
    record_id INTEGER,
    source_type TEXT,             -- 'xml_dump', 'api', 'manual'
    source_date DATE,
    dump_file TEXT,               -- XML dump filename if applicable
    api_endpoint TEXT,            -- API endpoint if applicable
    PRIMARY KEY (table_name, record_id)
);

CREATE TABLE sync_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    sync_type TEXT,               -- 'collection', 'wantlist', 'folders'
    last_sync DATETIME,
    records_processed INTEGER,
    status TEXT,                  -- 'success', 'error', 'partial'
    error_message TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Indexes for Performance

CREATE INDEX idx_artists_name ON artists(name);
CREATE INDEX idx_labels_name ON labels(name);
CREATE INDEX idx_releases_title ON releases(title);
CREATE INDEX idx_releases_year ON releases(year);
CREATE INDEX idx_releases_master ON releases(master_id);
CREATE INDEX idx_release_artists_release ON release_artists(release_id);
CREATE INDEX idx_release_artists_artist ON release_artists(artist_id);
CREATE INDEX idx_release_labels_release ON release_labels(release_id);
CREATE INDEX idx_release_labels_label ON release_labels(label_id);
CREATE INDEX idx_tracks_release ON tracks(release_id);
CREATE INDEX idx_user_collection_user ON user_collection(user_id);
CREATE INDEX idx_user_collection_release ON user_collection(release_id);
CREATE INDEX idx_user_collection_folder ON user_collection(folder_id);
CREATE INDEX idx_data_sources_table_record ON data_sources(table_name, record_id);
CREATE INDEX idx_sync_status_user ON sync_status(user_id);