# movies_queries.py
# Module 7 - MySQL Queries with Python

import mysql.connector

try:
    # Step 1: Connect to the database
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Pumpkin2024@",
        database="movies"
    )
    cursor = db.cursor()
    print("Connected to the database successfully!\n")

    # Step 2: Query 1 Display all fields from the studio table
    print("-- DISPLAYING Studio RECORDS --")
    query1 = "SELECT studio_id, name FROM studio;"
    cursor.execute(query1)
    studios = cursor.fetchall()
    for studio in studios:
        print(f"Studio ID: {studio[0]}")
        print(f"Studio Name: {studio[1]}\n")

    # Step 3: Query 2 - Display all fields from the genre table
    print("-- DISPLAYING Genre RECORDS --")
    query2 = "SELECT genre_id, name FROM genre;"
    cursor.execute(query2)
    genres = cursor.fetchall()
    for genre in genres:
        print(f"Genre ID: {genre[0]}")
        print(f"Genre Name: {genre[1]}\n")

    # Step 4: Query 3 - Display Movie names for films with runtime < 120
    print("-- DISPLAYING Short Movie RECORDS --")
    query3 = "SELECT name, runtime FROM movie WHERE runtime < 120;"
    cursor.execute(query3)
    movies = cursor.fetchall()  # ← fixed variable name here
    for movie in movies:
        print(f"Film Name: {movie[0]}")
        print(f"Runtime: {movie[1]}\n")

    # Step 5: Query 4 - Display film names and directors grouped by director
    print("-- DISPLAYING Director RECORDS in Order --")
    query4 = """
        SELECT name, director
        FROM movie
        ORDER BY director;
    """
    cursor.execute(query4)
    directors = cursor.fetchall()
    for director in directors:
        print(f"Film Name: {director[0]}")
        print(f"Director: {director[1]}\n")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # Step 6: Close connection
    if cursor:
        cursor.close()
    if db:
        db.close()

