from swh.storage import db

# ok!
with db.connect ('dbname=softwareheritage-dev') as db_conn:
    with db_conn.cursor() as cur:
        cur.execute("insert into swh.origin(type, url) values ('git', 'https://github.com/ardumont/dot-files2') returning id")
        r = cur.fetchone()
        print(r)

# ok!
with db.connect ('dbname=softwareheritage-dev') as db_conn:
    r = db.insert(db_conn, "insert into swh.origin(type, url) values ('git', 'https://github.com/ardumont/dot-files2') returning id")
    print(r)
