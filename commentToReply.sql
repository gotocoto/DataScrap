-- SQLite
SELECT c1.content, c2.content
FROM comment as c1, comment as c2
WHERE c1.id = c2.parent_id;
