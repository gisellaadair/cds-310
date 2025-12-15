"""
Bacchus Winery Reports
Generates:
1) Supplier delivery performance (monthly, late deliveries)
2) Wine sales & distribution report
3) Employee hours worked per quarter (last four quarters)
"""

import mysql.connector
from dotenv import dotenv_values

SECRETS = dotenv_values(".env")

CONFIG = {
    "user": SECRETS["USER"],
    "password": SECRETS["PASSWORD"],
    "host": SECRETS["HOST"],
    "database": SECRETS["DATABASE"],
    "raise_on_warnings": True,
}


# ---------------------------------------------------
# 1. Supplier Delivery Performance Report
# ---------------------------------------------------
def supplier_delivery_report(cursor):
    print("\n=== SUPPLIER DELIVERY PERFORMANCE (MONTHLY) ===")

    query = """
            SELECT
                s.supplier_name,
                sd.supply_item_type,
                sd.expected_delivery_date,
                sd.actual_delivery_date,
                DATEDIFF(sd.actual_delivery_date, sd.expected_delivery_date) AS days_late,
                MONTH(sd.expected_delivery_date) AS delivery_month
            FROM supply_deliveries sd
                JOIN suppliers s ON sd.supplier_id = s.supplier_id
            ORDER BY sd.expected_delivery_date; \
            """

    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
        supplier, item, expected, actual, days_late, month = row
        status = "ON TIME" if days_late <= 0 else f"LATE by {days_late} days"
        print(
            f"Month {month} | Supplier: {supplier} | Item: {item} | "
            f"Expected: {expected} | Actual: {actual} | {status}"
        )


# ---------------------------------------------------
# 2. Wine Sales & Distribution Report
# ---------------------------------------------------
def wine_distribution_report(cursor):
    print("\n=== WINE SALES & DISTRIBUTION REPORT ===")

    query = """
            SELECT
                w.wine_name,
                d.distributor_name,
                SUM(ws.quantity_shipped) AS total_shipped
            FROM wine_shipments ws
                     JOIN wines w ON ws.wine_id = w.wine_id
                     JOIN distributors d ON ws.distributor_id = d.distributor_id
            GROUP BY w.wine_name, d.distributor_name
            ORDER BY w.wine_name; \
            """

    cursor.execute(query)
    rows = cursor.fetchall()

    for wine, distributor, qty in rows:
        print(f"Wine: {wine} | Distributor: {distributor} | Quantity Shipped: {qty}")

    print("\n--- TOTAL SALES BY WINE ---")
    cursor.execute("""
                   SELECT w.wine_name, SUM(ws.quantity_shipped) AS total_shipped
                   FROM wine_shipments ws
                            JOIN wines w ON ws.wine_id = w.wine_id
                   GROUP BY w.wine_name
                   ORDER BY total_shipped ASC;
                   """)

    for wine, total in cursor.fetchall():
        print(f"Wine: {wine} | Total Shipped: {total}")


# ---------------------------------------------------
# 3. Employee Hours Worked (Last 4 Quarters)
# ---------------------------------------------------
def employee_hours_report(cursor):
    print("\n=== EMPLOYEE HOURS WORKED (BY QUARTER) ===")

    query = """
            SELECT
                e.employee_name,
                YEAR(te.work_date) AS year,
                QUARTER(te.work_date) AS quarter,
                SUM(te.hours_worked) AS total_hours
            FROM time_entries te
                JOIN employees e ON te.employee_id = e.employee_id
            GROUP BY e.employee_name, year, quarter
            ORDER BY e.employee_name, year, quarter; \
            """

    cursor.execute(query)
    rows = cursor.fetchall()

    for employee, year, quarter, hours in rows:
        print(
            f"Employee: {employee} | Year: {year} | Q{quarter} | Hours Worked: {hours}"
        )


# ---------------------------------------------------
# Main Runner
# ---------------------------------------------------
def main():
    cnx = mysql.connector.connect(**CONFIG)
    cursor = cnx.cursor()

    supplier_delivery_report(cursor)
    wine_distribution_report(cursor)
    employee_hours_report(cursor)

    cursor.close()
    cnx.close()


if __name__ == "__main__":
    main()