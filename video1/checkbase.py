import psycopg2
import cfg
con = psycopg2.connect(dbname=cfg.b_base, user=cfg.b_login,
                        password=cfg.b_pass, host=cfg.b_host)
cur = con.cursor()
"""
cur.execute('''CREATE TABLE dht
     ( temp float8,
      hum float8);''')

print("Table created successfully")
con.commit()  
con.close()
"""
cur.execute('SELECT * FROM dht')
for row in cur:
    print(row)

