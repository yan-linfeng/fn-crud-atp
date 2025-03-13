import io
import os
import json
import oracledb
from timeit import default_timer as timer
from fdk import response

# Get connection parameters from enviroment
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
dsn = os.getenv("DSN")

if db_user == None:
    raise ValueError("ERROR: Missing configuration key DBUSER")
if db_password == None:
    raise ValueError("ERROR: Missing configuration key DBPASSWORD")
if dsn == None:
    raise ValueError("ERROR: Missing configuration key DSN")

print("INFO: DB_USER is {}".format(db_user), flush=True)
print("INFO: db_password is {}".format(db_password), flush=True)
print("INFO: dsn is {}".format(dsn), flush=True)


# Create the DB Session Pool
start_pool = timer()
dbpool = oracledb.create_pool(user=db_user, password=db_password, dsn=dsn, min=1, max=10) 
end_pool = timer()
print("INFO: DB pool created in {} sec".format(end_pool - start_pool), flush=True)

#
# Function Handler: executed every time the function is invoked
#
def handler(ctx, data: io.BytesIO = None):
    try:
        user_id = None
        path = ctx.RequestURL()
        path_parts = path.strip('/').split('/')
        if len(path_parts) >= 2 and path_parts[-2] == 'users':
            user_id = path_parts[-1]

        if not user_id:
            return read_all_users(ctx)
        else:
            return read_user(ctx, user_id)
    except Exception as ex:
        print('ERROR: Invalid payload', ex, flush=True)
        raise
    

def read_user(ctx, user_id):
    try:
        sql_statement = """
            SELECT ID, FIRST_NAME, LAST_NAME, USERNAME, CREATED_ON
            FROM users
            WHERE ID = :1
        """
        bind_vars = [user_id]

        with dbpool.acquire() as dbconnection:
            with dbconnection.cursor() as dbcursor:
                dbcursor.execute(sql_statement, bind_vars)
                dbcursor.rowfactory = lambda *args: dict(zip([d[0] for d in dbcursor.description], args))
                result = dbcursor.fetchone()

                if result:
                    if result.get("CREATED_ON"):
                        result["CREATED_ON"] = result["CREATED_ON"].isoformat()

                    return response.Response(
                        ctx,
                        response_data=json.dumps(result),
                        headers={"Content-Type": "application/json"}
                    )
                else:
                    return response.Response(
                        ctx,
                        response_data=json.dumps({"message": "User not found"}),
                        headers={"Content-Type": "application/json"}
                    )

    except Exception as ex:
        print('ERROR: Failed to read user', ex, flush=True)
        raise

def read_all_users(ctx):
    try:
        sql_statement = """
            SELECT ID, FIRST_NAME, LAST_NAME, USERNAME, CREATED_ON
            FROM users
        """

        with dbpool.acquire() as dbconnection:
            with dbconnection.cursor() as dbcursor:
                dbcursor.execute(sql_statement)
                dbcursor.rowfactory = lambda *args: dict(zip([d[0] for d in dbcursor.description], args))
                results = dbcursor.fetchall()

                for result in results:
                    if result.get("CREATED_ON"):
                        result["CREATED_ON"] = result["CREATED_ON"].isoformat()

                return response.Response(
                    ctx,
                    response_data=json.dumps(results),
                    headers={"Content-Type": "application/json"}
                )

    except Exception as ex:
        print('ERROR: Failed to read all users', ex, flush=True)
        raise