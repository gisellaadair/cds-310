"""Create and populate Bacchus Winery schema in MySQL.
Defines tables, creates them in a MySQL database, and populates
them with initial data. Create the database beforehand and
set connection parameters in a .env file."""
import mysql.connector
from mysql.connector import errorcode
from dotenv import dotenv_values

SECRETS = dotenv_values(".env")
CONFIG = {
    "user": SECRETS["USER"],
    "password": SECRETS["PASSWORD"],
    "host": SECRETS["HOST"],
    "database": SECRETS["DATABASE"],
    "raise_on_warnings": True,
}

TABLES = {}

TABLES["suppliers"] = """
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id INT AUTO_INCREMENT PRIMARY KEY,
    supplier_name VARCHAR(100) NOT NULL
) ENGINE=InnoDB;
"""

TABLES["wines"] = """
CREATE TABLE IF NOT EXISTS wines (
    wine_id INT AUTO_INCREMENT PRIMARY KEY,
    wine_name VARCHAR(100) NOT NULL
) ENGINE=InnoDB;
"""

TABLES["distributors"] = """
CREATE TABLE IF NOT EXISTS distributors (
    distributor_id INT AUTO_INCREMENT PRIMARY KEY,
    distributor_name VARCHAR(100) NOT NULL
) ENGINE=InnoDB;
"""

TABLES["employees"] = """
CREATE TABLE IF NOT EXISTS employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_name VARCHAR(100) NOT NULL
) ENGINE=InnoDB;
"""

TABLES["supply_deliveries"] = """
CREATE TABLE IF NOT EXISTS supply_deliveries (
    supply_delivery_id INT AUTO_INCREMENT PRIMARY KEY,
    supplier_id INT NOT NULL,
    supply_item_type VARCHAR(50) NOT NULL,
    expected_delivery_date DATE NOT NULL,
    actual_delivery_date DATE NOT NULL,
    quantity_delivered INT NOT NULL,
    CONSTRAINT fk_supply_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES suppliers (supplier_id)
        ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;
"""

TABLES["wine_shipments"] = """
CREATE TABLE IF NOT EXISTS wine_shipments (
    wine_shipment_id INT AUTO_INCREMENT PRIMARY KEY,
    wine_id INT NOT NULL,
    distributor_id INT NOT NULL,
    shipment_date DATE NOT NULL,
    quantity_shipped INT NOT NULL,
    CONSTRAINT fk_shipment_wine
        FOREIGN KEY (wine_id)
        REFERENCES wines (wine_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_shipment_distributor
        FOREIGN KEY (distributor_id)
        REFERENCES distributors (distributor_id)
        ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;
"""

TABLES["time_entries"] = """
CREATE TABLE IF NOT EXISTS time_entries (
    time_entry_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id INT NOT NULL,
    work_date DATE NOT NULL,
    hours_worked DECIMAL(5,2) NOT NULL,
    CONSTRAINT fk_time_employee
        FOREIGN KEY (employee_id)
        REFERENCES employees (employee_id)
        ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;
"""


def create_tables(cursor):
    # Drop child tables first (to avoid FK issues), then parents.
    drop_order = [
        "time_entries",
        "wine_shipments",
        "supply_deliveries",
        "employees",
        "distributors",
        "wines",
        "suppliers",
    ]
    for name in drop_order:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {name};")
        except mysql.connector.Error as err:
            print(f"Failed to drop table {name}: {err}")

    # Create parent tables first, then children.
    create_order = [
        "suppliers",
        "wines",
        "distributors",
        "employees",
        "supply_deliveries",
        "wine_shipments",
        "time_entries",
    ]
    for name in create_order:
        cursor.execute(TABLES[name])


def populate_suppliers(cursor):
    # Case mentions 3 suppliers (bottles/corks, labels/boxes, vats/tubing).
    suppliers = [
        ("Bottle & Cork Co.",),
        ("Label & Box Inc.",),
        ("Vats & Tubing Ltd.",),
    ]
    cursor.executemany(
        "INSERT INTO suppliers (supplier_name) VALUES (%s);",
        suppliers,
    )


def populate_wines(cursor):
    # Six wines from the case plus two plausible variants.
    wines = [
        ("Merlot",),
        ("Cabernet",),
        ("Chablis",),
        ("Chardonnay",),
        ("Merlot Reserve",),
        ("Cabernet Reserve",),
    ]
    cursor.executemany(
        "INSERT INTO wines (wine_name) VALUES (%s);",
        wines,
    )


def populate_distributors(cursor):
    distributors = [
        ("Atlantic Wine Distributors",),
        ("Pacific Fine Wines",),
        ("Midwest Beverage Co.",),
        ("Southern Cellars",),
        ("Northern Spirits",),
        ("Coastal Wine Group",),
    ]
    cursor.executemany(
        "INSERT INTO distributors (distributor_name) VALUES (%s);",
        distributors,
    )


def populate_employees(cursor):
    # Named employees from the case plus two generic production workers.
    employees = [
        ("Janet Collins",),    # finance and payroll
        ("Roz Murphy",),       # marketing head
        ("Bob Ulrich",),       # marketing assistant
        ("Henry Doyle",),      # production manager
        ("Maria Costanza",),   # distribution manager
        ("Jon Doe",),          # production worker 1
        ("Jane Smith",),       # production worker 2
    ]
    cursor.executemany(
        "INSERT INTO employees (employee_name) VALUES (%s);",
        employees,
    )


def populate_supply_deliveries(cursor):
    # Assume auto-increment IDs: suppliers 1, 2, 3.
    deliveries = [
        # supplier_id, supply_item_type, expected_date, actual_date, qty
        (1, "BOTTLES", "2025-01-10", "2025-01-10", 10000),
        (1, "CORKS", "2025-01-10", "2025-01-12", 10000),
        (2, "LABELS", "2025-02-05", "2025-02-06", 8000),
        (2, "BOXES", "2025-02-05", "2025-02-05", 8000),
        (3, "VATS", "2025-03-01", "2025-03-15", 2),
        (3, "TUBING", "2025-03-01", "2025-03-03", 500),
    ]
    cursor.executemany(
        """
        INSERT INTO supply_deliveries
            (supplier_id, supply_item_type, expected_delivery_date,
             actual_delivery_date, quantity_delivered)
        VALUES (%s, %s, %s, %s, %s);
        """,
        deliveries,
    )


def populate_wine_shipments(cursor):
    # Assume wines 1–6, distributors 1–6.
    shipments = [
        # wine_id, distributor_id, shipment_date, quantity_shipped
        (1, 1, "2025-01-15", 300),   # Merlot to Atlantic
        (2, 2, "2025-01-18", 250),   # Cabernet to Pacific
        (3, 3, "2025-01-20", 150),   # Chablis to Midwest
        (4, 4, "2025-01-22", 200),   # Chardonnay to Southern
        (1, 5, "2025-02-05", 220),   # Merlot to Northern
        (2, 6, "2025-02-10", 260),   # Cabernet to Coastal
        (4, 1, "2025-02-12", 180),   # Chardonnay to Atlantic
        (3, 2, "2025-02-18", 130),   # Chablis to Pacific
    ]
    cursor.executemany(
        """
        INSERT INTO wine_shipments
            (wine_id, distributor_id, shipment_date, quantity_shipped)
        VALUES (%s, %s, %s, %s);
        """,
        shipments,
    )


def populate_time_entries(cursor):
    # Assume employees 1–7.
    # Several entries across different quarters.
    entries = [
        # employee_id, work_date, hours_worked
        (1, "2025-01-05", 8.00),
        (1, "2025-01-06", 7.50),
        (2, "2025-01-05", 8.00),
        (3, "2025-01-05", 6.00),
        (4, "2025-01-05", 9.00),
        (5, "2025-01-05", 8.00),
        (6, "2025-01-05", 8.00),
        (6, "2025-04-10", 8.00),
        (7, "2025-04-10", 8.00),
        (4, "2025-07-15", 9.00),
        (6, "2025-07-15", 8.00),
        (7, "2025-10-20", 8.00),
    ]
    cursor.executemany(
        """
        INSERT INTO time_entries
            (employee_id, work_date, hours_worked)
        VALUES (%s, %s, %s);
        """,
        entries,
    )

def show_data(cursor, tables=None):
    """
    Display records from one or more tables, showing column names and values.

    Args:
        cursor: MySQL cursor object.
        tables: None, a string table name, or a list of table names.
                - None → show all tables defined in TABLES
                - "suppliers" → show one table
                - ["suppliers", "employees"] → show subset
    """

    # Normalize parameter to a list
    if tables is None:
        tables = list(TABLES.keys())
    elif isinstance(tables, str):
        tables = [tables]

    for table_name in tables:
        print(f"\n=== Data in table '{table_name}' ===")

        # Query table
        cursor.execute(f"SELECT * FROM `{table_name}`;")
        rows = cursor.fetchall()

        # Get column names from cursor description
        col_names = [col[0] for col in cursor.description]

        if not rows:
            print("(No rows)")
            continue

        # Display rows with key/value pairs
        for row in rows:
            row_dict = dict(zip(col_names, row))
            for key, value in row_dict.items():
                print(f"{key}: {value}")
            print("---")

def main():
    try:
        # ===========================================================
        # ADDED — create the database if it does not already exist
        root_connection = mysql.connector.connect(
            user=SECRETS["USER"],
            password=SECRETS["PASSWORD"],
            host=SECRETS["HOST"]
        )
        root_cursor = root_connection.cursor()
        root_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SECRETS['DATABASE']};")
        root_cursor.close()
        root_connection.close()
        # ===========================================================

        cnx = mysql.connector.connect(**CONFIG)
        cursor = cnx.cursor()

        create_tables(cursor)
        populate_suppliers(cursor)
        populate_wines(cursor)
        populate_distributors(cursor)
        populate_employees(cursor)
        populate_supply_deliveries(cursor)
        populate_wine_shipments(cursor)
        populate_time_entries(cursor)

        cnx.commit()
        show_data(cursor)
        cursor.close()
        cnx.close()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Access denied: check your USER and PASSWORD in .env.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist: check DATABASE in .env.")
        else:
            print(f"MySQL error: {err}")


if __name__ == "__main__":
    main()