 import sys, psycopg2

    conn = psycopg2.connect("dbname='dvdrental' user='postgres' host='localhost' password='root'")
    cur = conn.cursor()
    print('Connecting to Database')
    sql = "COPY (SELECT film.film_id, title, inventory_id FROM film LEFT JOIN inventory ON inventory.film_id = film.film_id ORDER BY title ) TO STDOUT WITH CSV DELIMITER ','"
    with open("/home/hduser/psql.csv", "w") as file:
    cur.copy_expert(sql, file)
    cur.close()
    print('CSV File has been created')