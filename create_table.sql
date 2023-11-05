CREATE TABLE IF NOT EXISTS elisasuaezmoreira_coderhouse.PrimerEntregable
(
    article_id VARCHAR(50) PRIMARY KEY distkey,
    title VARCHAR(100),
    creator VARCHAR(50),
    link VARCHAR(100),
    pubDate DATE,
    language VARCHAR(50),
    category VARCHAR(50),
    country VARCHAR(50),
    description VARCHAR(500)
)
SORTKEY (pubDate);
