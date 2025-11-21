import mysql.connector  # Connect python with MySQL

# Connect to the Database
try:
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Pumpkin2024@",
        database="movies"
    )
    cursor = db.cursor()
    print("Conectado a la base de datos correctamente!\n")

    # Show movies
    def show_films(cursor, title):
        print("\n-- {} --".format(title))
        
        query = (
            "SELECT movie.name AS Name, "
            "movie.director AS Director, "
            "genre.name AS Genre, "
            "studio.name AS Studio "
            "FROM movie "
            "INNER JOIN genre ON movie.genre_id = genre.genre_id "
            "INNER JOIN studio ON movie.studio_id = studio.studio_id"
        )
        
        try:
            cursor.execute(query)
            films = cursor.fetchall()
            for film in films:
                print(f"Film Name: {film[0]}\nDirector: {film[1]}\nGenre: {film[2]}\nStudio: {film[3]}\n")
        except Exception as e:
            print(f"Error mostrando películas: {e}")

    # Show movies before changes
    show_films(cursor, "DISPLAYING MOVIES BEFORE CHANGES")

    # Insert a movie
    try:
        cursor.execute(
            "INSERT INTO movie (name, director, genre_id, studio_id) "
            "VALUES ('Inception', 'Christopher Nolan', 1, 2)"
        )
        db.commit()
        show_films(cursor, "DISPLAYING MOVIES AFTER INSERT")
    except Exception as e:
        print(f"Error insertando película: {e}")

    # Upsate the director
    try:
        cursor.execute(
            "UPDATE movie SET director = 'Ridley Scott' WHERE name = 'Alien'"
        )
        db.commit()
        show_films(cursor, "DISPLAYING MOVIES AFTER UPDATE")
    except Exception as e:
        print(f"Error actualizando película: {e}")

    # Delete movie
    try:
        cursor.execute(
            "DELETE FROM movie WHERE name = 'Gladiator'"
        )
        db.commit()
        show_films(cursor, "DISPLAYING MOVIES AFTER DELETE")
    except Exception as e:
        print(f"Error eliminando película: {e}")

# Conection error
except mysql.connector.Error as err:
    print(f"Error de conexión: {err}")

#Close connection
finally:
    if cursor:
        cursor.close()
    if db:
        db.close()
