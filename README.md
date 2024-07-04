# Database



## Article

| Column  | Data Type      | Constraints                   | Description                                   |
|---------|----------------|-------------------------------|-----------------------------------------------|
| URL     | VARCHAR(230)   | PRIMARY KEY                   | URL to the article                            |
| Category| VARCHAR(30)    |                               | Category of the aricle                        |
| LastMod | TIMESTAMP      | NOT NULL                      | Last time article edited by news website      |
| Scraped | TIMESTAMP      | DEFAULT NULL                  | Time when the article was last scrapped       |
| Title   | VARCHAR(255)   |                               | Title of the article                          |
| Author  | VARCHAR(30)    |                               | Author of the article                         |


```sql
CREATE TABLE Articles (
    URL VARCHAR(230) PRIMARY KEY,
    Category VARCHAR(20),
    LastMod TIMESTAMP NOT NULL,
    Scraped TIMESTAMP DEFAULT NULL,
    Title VARCHAR(255),
    Author VARCHAR(30)
);
```
## User
Certainly! Below is the MySQL `CREATE TABLE` statement for the `user` table along with its Markdown description:

### MySQL `CREATE TABLE` Statement:
```sql
CREATE TABLE user (
    id VARCHAR(255) PRIMARY KEY,
    user_name VARCHAR(255),
    received_ranked_up VARCHAR(255),
    total VARCHAR(255)
);
```

### Markdown Table Description:

| Column           | Data Type   | Constraints | Description                           |
|------------------|-------------|-------------|---------------------------------------|
| id               | VARCHAR(255)| PRIMARY KEY | Unique identifier for the user        |
| user_name        | VARCHAR(255)|             | Name of the user                      |
| received_ranked_up| VARCHAR(255)|             | Number of received ranked up points   |
| total            | VARCHAR(255)|             | Total points or score for the user     |

```sql
CREATE TABLE user (
    id VARCHAR(255) PRIMARY KEY,
    user_name VARCHAR(255),
    received_ranked_up VARCHAR(255),
    total VARCHAR(255)
);
```
## Comment
| Column Name      | Data Type  | Description                                      |
|------------------|------------|--------------------------------------------------|
| article          | TEXT       | The ID of the related article                    |
| root_comment     | TEXT       | The ID of the root comment (if applicable)       |
| parent_id        | TEXT       | The ID of the parent comment (if applicable)     |
| depth            | INTEGER    | The depth level of the comment in the thread     |
| id               | TEXT (PK)  | The unique identifier for the comment (Primary Key) |
| user_id          | TEXT       | The ID of the user who posted the comment        |
| time             | INTEGER    | The timestamp of when the comment was posted     |
| replies_count    | INTEGER    | The number of replies to this comment            |
| ranks_up         | INTEGER    | The number of upvotes for the comment            |
| ranks_down       | INTEGER    | The number of downvotes for the comment          |
| rank_score       | INTEGER    | The ranking score of the comment                  |
| content          | TEXT       | The textual content of the comment               |
| user_reputation  | INTEGER    | The reputation score of the commenting user      |
| best_score       | INTEGER    | The best score achieved by the comment           |


```sql
CREATE TABLE comment (
    article TEXT,
    root_comment TEXT,
    parent_id TEXT,
    depth INTEGER,
    id TEXT PRIMARY KEY,
    user_id TEXT,
    time INTEGER,
    replies_count INTEGER,
    ranks_up INTEGER,
    ranks_down INTEGER,
    rank_score INTEGER,
    content TEXT,
    user_reputation INTEGER,
    best_score INTEGER,
    FOREIGN KEY (article) REFERENCES article(id) ON DELETE CASCADE ON UPDATE CASCADE,
    
);
```
