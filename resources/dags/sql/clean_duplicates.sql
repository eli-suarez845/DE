merge into {table_name}
using stage_{table_name}
ON {table_name}.article_id = stage_{table_name}.article_id
WHEN MATCHED THEN
UPDATE SET
 article_id = stage_{table_name}.article_id,
 pubDate = stage_{table_name}.pubDate,
 title = stage_{table_name}.title,
 creator = stage_{table_name}.creator,
 category = stage_{table_name}.category,
 country = stage_{table_name}.country,
 language = stage_{table_name}.language,
 link = stage_{table_name}.link,
 description = stage_{table_name}.description
WHEN NOT MATCHED THEN
 INSERT VALUES (stage_{table_name}.article_id, stage_{table_name}.pubDate,stage_{table_name}.title,
 stage_{table_name}.creator,stage_{table_name}.category, stage_{table_name}.country,stage_{table_name}.language,
 stage_{table_name}.link,stage_{table_name}.description);

TRUNCATE stage_{table_name};