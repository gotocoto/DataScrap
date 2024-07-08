# Database



## article

| Column  | Data Type      | Constraints                   | Description                                   |
|---------|----------------|-------------------------------|-----------------------------------------------|
| URL     | VARCHAR(230)   | PRIMARY KEY                   | URL to the article                            |
| Category| VARCHAR(30)    |                               | Category of the aricle                        |
| LastMod | TIMESTAMP      | NOT NULL                      | Last time article edited by news website      |
| Scraped | TIMESTAMP      | DEFAULT NULL                  | Time when the article was last scrapped       |
| Title   | VARCHAR(255)   |                               | Title of the article                          |
| Author  | VARCHAR(30)    |                               | Author of the article                         |
| post_id    | VARCHAR(30)    |                               | Author of the article                         |

```sql
CREATE TABLE `article` (
    `url` VARCHAR(230) PRIMARY KEY,
    `category` VARCHAR(30),
    `last_mod` TIMESTAMP NOT NULL,
    `scraped` TIMESTAMP DEFAULT NULL,
    `title` VARCHAR(255) DEFAULT NULL,
    `author` VARCHAR(60) DEFAULT NULL,
    `post_id` CHAR(36) DEFAULT NULL UNIQUE
);

```

### Category
```sql
SELECT count(category),Category FROM Articles GROUP BY Category;
```
| Count           | Category               |
|-----------------|------------------------|
|              10 |                        |
|           11001 | auto                   |
|           89836 | entertainment          |
|             634 | faith-values           |
|              12 | family                 |
|           15109 | food-drink             |
|               1 | forum                  |
|               1 | fox-and-friends        |
|               1 | fox-friends            |
|               1 | games                  |
|            1117 | great-outdoors         |
|           48061 | health                 |
|           19461 | lifestyle              |
|           49030 | media                  |
|               2 | mediabuzz              |
|               3 | midterms-2018          |
|             154 | official-polls         |
|           37121 | opinion                |
|          143157 | politics               |
|            1619 | real-estate            |
|           17261 | science                |
|             289 | shows                  |
|          313748 | sports                 |
|          229399 | story                  |
|               1 | sunday-morning-futures |
|           15436 | tech                   |
|           36511 | transcript             |
|           12267 | travel                 |
|          247542 | us                     |
|             439 | weather                |
|          237713 | world                  |

```sql
SELECT url FROM article WHERE YEAR(last_mod)=2020 and category='politics' ORDER BY last_mod LIMIT 5 FOR UPDATE ;
```
```sql
UPDATE article
SET scraped=CURDATE(),title=?,author=?
WHERE url=?;
```

## User
| Column           | Data Type   | Constraints | Description                           |
|------------------|-------------|-------------|---------------------------------------|
| id               | VARCHAR(255)| PRIMARY KEY | Unique identifier for the user        |
| user_name        | VARCHAR(50)|             | Name of the user                      |
| received_ranked_up| integer|             | Number of received ranked up points   |
| total            | integer|             | Total points or score for the user     |

```sql
CREATE TABLE user (
    id CHAR(14) PRIMARY KEY,
    user_name VARCHAR(50) NOT NULL,
    received_ranked_up INT UNSIGNED DEFAULT 0,
    total INT UNSIGNED DEFAULT 0,
    is_admin TINYINT(1) DEFAULT 0,
    is_community_moderator TINYINT(1) DEFAULT 0,
    is_super_admin TINYINT(1) DEFAULT 0,
    is_journalist TINYINT(1) DEFAULT 0,
    is_muted TINYINT(1) DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```
## Comment
| Column Name   | Data Type      | Nullable | Description                                           |
|---------------|----------------|----------|-------------------------------------------------------|
| post_id       | CHAR(36)       | No       | The ID of the related article                         |
| root_comment  | CHAR(27)       | No       | The ID of the root comment (if applicable)            |
| parent_id     | CHAR(27)       | Yes      | The ID of the parent comment (if applicable)          |
| depth         | INTEGER        | No       | The depth level of the comment in the thread          |
| id            | CHAR(27)       | No       | The unique identifier for the comment (Primary Key)   |
| user_id       | CHAR(14)       | No       | The ID of the user who posted the comment             |
| time          | TIMESTAMP      | No       | The timestamp of when the comment was posted          |
| replies_count | INTEGER        | No       | The number of replies to this comment                 |
| ranks_up      | INTEGER        | No       | The number of upvotes for the comment                 |
| ranks_down    | INTEGER        | No       | The number of downvotes for the comment               |
| rank_score    | INTEGER        | No       | The ranking score of the comment                      |
| content       | TEXT           | No       | The textual content of the comment                    |
| user_reputation| INTEGER       | No       | The reputation score of the commenting user           |
| best_score    | INTEGER        | No       | The best score achieved by the comment                |



```sql
CREATE TABLE comment (
    post_id CHAR(36),
    root_comment VARCHAR(30),
    parent_id VARCHAR(30) NULL,
    depth INT UNSIGNED DEFAULT 0,
    id VARCHAR(30) PRIMARY KEY,
    user_id CHAR(14),
    time DATETIME,
    replies_count INT UNSIGNED DEFAULT 0,
    ranks_up INT UNSIGNED DEFAULT 0,
    ranks_down INT UNSIGNED DEFAULT 0,
    rank_score INT UNSIGNED DEFAULT 0,
    content TEXT,
    user_reputation INT UNSIGNED DEFAULT 0,
    best_score INT UNSIGNED DEFAULT 0,
    FOREIGN KEY (post_id) REFERENCES article(post_id) ON DELETE CASCADE ON UPDATE NO ACTION,
    FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE ON UPDATE NO ACTION
);

```
,
    --CONSTRAINT fk_parent_id FOREIGN KEY (parent_id) REFERENCES comment(id) ON DELETE CASCADE ON UPDATE NO ACTION,
    --CONSTRAINT fk_root_comment FOREIGN KEY (root_comment) REFERENCES comment(id) ON DELETE CASCADE ON UPDATE NO ACTION