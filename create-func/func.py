import io
import os
import json
import oracledb
from timeit import default_timer as timer
from fdk import response

if os.getenv("DBUSER") != None:
    dbuser = os.getenv("DBUSER")
else:
    raise ValueError("ERROR: Missing configuration key DBUSER")
if os.getenv("DBPWD_CYPHER") != None:
    dbpwd_cypher = dbpwd = os.getenv("DBPWD_CYPHER") # The decryption of the db password using OCI KMS would have to be done, however it is not addressed here
else:
    raise ValueError("ERROR: Missing configuration key DBPWD_CYPHER")
if os.getenv("DSN") != None:
    dbsvc = os.getenv("DSN")
else:
    raise ValueError("ERROR: Missing configuration key DSN")

# Create the DB Session Pool
start_pool = timer()
dbpool = oracledb.create_pool(user=dbuser, password=dbpwd, dsn=dbsvc, min=1, max=10) 
end_pool = timer()
print("INFO: DB pool created in {} sec".format(end_pool - start_pool), flush=True)

#
# Function Handler: executed every time the function is invoked
#
def handler(ctx, data: io.BytesIO = None):
    try:
        payload_bytes = data.getvalue()
        if payload_bytes == b'':
            raise KeyError('No keys in payload')
        payload = json.loads(payload_bytes)

        return create_user(ctx, payload)
    except Exception as ex:
        print('ERROR: Invalid payload', ex, flush=True)
        raise
    
def create_user(ctx, payload):
    try:
        user_id = None
        path = ctx.RequestURL()
        path_parts = path.strip('/').split('/')
        if len(path_parts) >= 2 and path_parts[-2] == 'users':
            user_id = path_parts[-1]

        if not user_id:
            raise ValueError("Missing required fields: user_id")

        first_name = payload.get("first_name")
        last_name = payload.get("last_name")
        username = payload.get("username")

        if not first_name or not last_name or not username:
            raise ValueError("Missing required fields: first_name, last_name, username")

        sql_statement = """
            INSERT INTO users (ID, FIRST_NAME, LAST_NAME, USERNAME)
            VALUES (:1, :2, :3, :4)
        """
        bind_vars = [user_id, first_name, last_name, username]

    except Exception as ex:
        print('ERROR: Invalid payload', ex, flush=True)
        raise

    try:
        with dbpool.acquire() as dbconnection:
            with dbconnection.cursor() as dbcursor:
                dbcursor.execute(sql_statement, bind_vars)
                dbconnection.commit()
                return response.Response(
                    ctx,
                    response_data=json.dumps({"message": "User created successfully"}),
                    headers={"Content-Type": "application/json"}
                )
    except Exception as ex:
        print('ERROR: Failed to create user', ex, flush=True)
        raise
