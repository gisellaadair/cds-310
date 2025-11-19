import mysql.connector

def run_query(cursor, description, query, print_single_column=False):
    print(description)
    cursor.execute(query)
    results = cursor.fetchall()

    for row in results:
        if print_single_column:
            print(row[0])
        else:
            print(row)
    print("\n")


try:
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Pumpkin2024@",
        database="movies"
    )
    cursor = db.cursor()
    print("Connected to the database successfully!\n")

    # Query 1
    run_query(cursor, 
              "Query 1: All fields from the studio table", 
              "SELECT * FROM studio;")

    # Query 2
    run_query(cursor, 
              "Query 2: All fields from the genre table", 
              "SELECT * FROM genre;")

    # Query 3
    run_query(cursor, 
              "Query 3: Movies with runtime less than 2 hours", 
              "SELECT name FROM movie WHERE runtime < 120;", 
              print_single_column=True)

    # Query 4
    print("Query 4: Movies grouped by director")
    query4 = """
    SELECT director, GROUP_CONCAT(name SEPARATOR ', ') AS movies
    FROM movie
    GROUP BY director;
    """
    cursor.execute(query4)
    results4 = cursor.fetchall()
    for row in results4:
        print(f"Director: {row[0]}, Movies: {row[1]}")
    print("\n")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    if cursor:
        cursor.close()
    if db:
        db.close()
    print("Database connection closed.")

