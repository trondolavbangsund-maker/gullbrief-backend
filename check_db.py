import sqlite3

conn = sqlite3.connect("data/app.db")
conn.row_factory = sqlite3.Row

rows = conn.execute("""
select api_key,email,status,stripe_customer_id,stripe_subscription_id,created_at
from api_keys
order by created_at desc
limit 5
""").fetchall()

for r in rows:
    print(dict(r))

conn.close()