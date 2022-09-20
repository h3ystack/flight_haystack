import psycopg2.extras
from config.haystack_config import host, database, user

conn = None
try:
    conn = psycopg2.connect(host=host, database=database, user=user)
    db_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    print('Connected')

    # close the communication with the PostgreSQL
except (Exception, psycopg2.DatabaseError) as error:
    print('Could not connect: ', str(error))



# Open and read the file as a single buffer
fd = open('./sql/database_creation.sql', 'r')
sqlFile = fd.read()
fd.close()

try:
    db_cursor.execute(sqlFile)
    conn.commit()
    print('Successfully created database')
except Exception as error:
    print(str(error))
    print('Database creation failed')
