CREATE TABLE IF NOT EXISTS {schema}.{table_name}(
    article_id VARCHAR(50) PRIMARY KEY distkey,
    pubDate DATE,
    title VARCHAR(500),
    creator SUPER,
    category SUPER,
    country SUPER,
    language VARCHAR(100),
    link VARCHAR(1000),
    description VARCHAR(65535)
)
SORTKEY (pubDate);

CREATE TABLE IF NOT EXISTS {schema}.stage_{table_name}(
    article_id VARCHAR(50) PRIMARY KEY distkey,
    pubDate DATE,
    title VARCHAR(500),
    creator SUPER,
    category SUPER,
    country SUPER,
    language VARCHAR(100),
    link VARCHAR(1000),
    description VARCHAR(65535)
)
SORTKEY (pubDate);