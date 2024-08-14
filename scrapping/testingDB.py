import json
import mysql.connector
from datetime import datetime
# Load database configuration from file
with open('db_config.json', 'r') as config_file:
    config = json.load(config_file)

# Connect to MySQL database
try:
    connection = mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )
    
    if connection.is_connected():
        print("Connected to the database")
        # Your database operations here
        cursor = connection.cursor() 
        comments = [
            [
                '2a60b6a8-f8a4-5b8a-9412-0a296c59a02c',  # post_id
                '2Yp2G7PcnHDcdSBvPkmMPlZz3f9',  # root_comment
                '',  # parent_id
                0,  # depth
                '2Yp2G7PcnHDcdSBvPkmMPlZz3f9',  # id
                'u_WkCQjrms2F2X',  # user_id
                datetime(2023, 11, 28, 16, 48, 21),  # time
                1,  # replies_count
                0,  # ranks_up
                0,  # ranks_down
                0,  # rank_score
                '<p>HAHAHAHAHA  </p><p>&#34;to address the impacts of <a href="https://www.foxnews.com/category/world/environment/climate-change" target="_blank" rel="noopener">climate change</a>, and to pursue federal funds.&#34;</p><p>It&#39;s always about pursuing federal funds.</p><p></p><p>&#34;Massachusetts could see sea level rise by up to 2.5 feet by 2050 compared to 2008 if global emissions aren&#39;t dramatically reduced,&#34;</p><p>They COULD also see a rise of 500 FEET!!!  Who knows?!?!  But they need more federal funding NOW.</p>',  # content
                27900,  # user_reputation
                491  # best_score
        ]]
        insert_comments = """
            INSERT INTO comment 
            (post_id, root_comment, parent_id, depth, id, user_id, time, replies_count, ranks_up, ranks_down, rank_score, content, user_reputation, best_score) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            post_id = VALUES(post_id),
            root_comment = VALUES(root_comment),
            parent_id = VALUES(parent_id),
            depth = VALUES(depth),
            user_id = VALUES(user_id),
            time = VALUES(time),
            replies_count = VALUES(replies_count),
            ranks_up = VALUES(ranks_up),
            ranks_down = VALUES(ranks_down),
            rank_score = VALUES(rank_score),
            content = VALUES(content),
            user_reputation = VALUES(user_reputation),
            best_score = VALUES(best_score);
        """
        cursor.executemany(insert_comments, comments)

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if connection.is_connected():
        connection.close()
        print("Connection closed")
