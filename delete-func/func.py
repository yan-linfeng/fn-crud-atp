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
            raise ValueError("Missing required field: user_id")

        return delete_user(ctx, user_id)
    except Exception as ex:
        print('ERROR: Invalid payload', ex, flush=True)
        raise
    

def delete_user(ctx, user_id):
    try:

        sql_statement = """
            DELETE FROM users
            WHERE ID = :1
        """
        bind_vars = [user_id]

        with dbpool.acquire() as dbconnection:
            with dbconnection.cursor() as dbcursor:
                dbcursor.execute(sql_statement, bind_vars)
                dbconnection.commit()
                return response.Response(
                    ctx,
                    response_data=json.dumps({"message": "User deleted successfully"}),
                    headers={"Content-Type": "application/json"}
                )

    except Exception as ex:
        print('ERROR: Failed to delete user', ex, flush=True)
        raise