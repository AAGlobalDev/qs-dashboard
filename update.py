# -*- coding: utf-8 -*-
import psycopg2


def save2pg(reviews):
    con = psycopg2.connect(host="database-1.clyjrfzdyg83.us-east-2.rds.amazonaws.com",
                           port=5432, database="postgres", user="postgres", password="superstar123")

    ############ Drop Table If Exists...
    cur = con.cursor()
    cur.execute('''DROP TABLE IF EXISTS REVIEWS;''')
    con.commit()

    cur = con.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS REVIEWS
          (id_review      TEXT,
          caption         TEXT,
          relative_date   CHAR(50),
          rating          FLOAT,
          username        TEXT,
          n_review_user   INT,
          url_user        TEXT);''')
    con.commit()
    for review in reviews:
        cmt = review['caption'].replace("'", "''")
        cur.execute(
            "INSERT INTO REVIEWS (id_review, caption, relative_date, rating, username, n_review_user, url_user) "
            "VALUES ('{0}', '{1}', '{2}', {3}, '{4}', {5}, '{6}')".format(review['id_review'], cmt,
                                                                        review['relative_date'], review['rating'],
                                                                        review['username'], review['n_review_user'],
                                                                        review['url_user']));
        con.commit()

    con.commit()
    con.close()



