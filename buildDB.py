import sqlite3

conn = sqlite3.connect('redis_aof.db')

c = conn.cursor()
c.execute('''CREATE TABLE KEY_INFO
            (KEY_NAME TEXT PRIMARY KEY   NOT NULL,
            TYPE                CHAR(50) NOT NULL,
            ENCODING            CHAR(50),
            BYTE                INT,
            LENGTH              INT);''')

c.execute('''CREATE TABLE STRING_KEY
            (KEY_NAME   TEXT  PRIMARY KEY    NOT NULL,
                VALUE   TEXT    NOT NULL);''')

c.execute('''CREATE TABLE LIST_KEY
            (KEY_NAME   TEXT PRIMARY KEY    NOT NULL,
                VALUE   TEXT    NOT NULL
                );''')


c.execute('''CREATE TABLE HASH_KEY
            (KEY_NAME   TEXT    NOT NULL,
            CHILD_KEY   TEXT    NOT NULL,
                VALUE   TEXT    NOT NULL,
                LEN1    INT,
                LEN2    INT,
                PRIMARY KEY(KEY_NAME,CHILD_KEY));''')
c.execute('''CREATE INDEX index_hash on HASH_KEY (KEY_NAME);''')


c.execute('''CREATE TABLE SET_KEY
            (KEY_NAME   TEXT    NOT NULL,
                VALUE   TEXT    NOT NULL,
                PRIMARY KEY(KEY_NAME,VALUE));''')
c.execute('''CREATE INDEX index_set on SET_KEY (KEY_NAME);''')


c.execute('''CREATE TABLE ZSET_KEY
            (KEY_NAME   TEXT    NOT NULL,
                VALUE   TEXT    NOT NULL,
                SCORE   REAL,
                LEN1    INT,
                PRIMARY KEY(KEY_NAME,VALUE));''')
c.execute('''CREATE INDEX index_zset on ZSET_KEY (KEY_NAME);''')


conn.commit()
conn.close()








