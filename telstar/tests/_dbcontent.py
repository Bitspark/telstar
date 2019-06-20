import os
from playhouse.db_url import connect
db = connect(os.environ["DATABASE"])
result = db.execute_sql("SELECT number, group_name FROM test")
print("\n".join(map(str, list(result))))
