import logging
from aiomariadb import Connection


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    conn = Connection()

    # Connect to the server
    status = conn.connect_start('localhost', 'tuomas', 'password', 'employees', 3306)
    while status:
        status = conn.wait(status)
        status = conn.connect_cont(status)

    # Query
    status = conn.query_start('SELECT * from employees;')
    while status:
        status = conn.wait(status)
        status = conn.query_cont(status)

    # Fetch a cursor for incoming data. use_result = streaming, store_result = fetch all first
    cursor = conn.store_result()

    # We want a single row, initialize one and fill it
    while True:
        status = cursor.fetch_row_start()
        while status:
            status = cursor.wait(status)
            status = cursor.fetch_row_cont(status)

        if cursor.is_finished:
            break

        print(cursor.row)

    # Close connection. This MAY block if socket is full, but generally shouldn't.
    status = conn.close_start()
    while status:
        status = conn.wait(status)
        status = conn.close_cont(status)

