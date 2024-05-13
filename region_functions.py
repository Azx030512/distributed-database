import pymysql.cursors
import json

def create_region_table(host, user, password, database, create_table_query):

    response = {"success": 0, "message": ""}

    try:
        # 连接到数据库
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            cursorclass=pymysql.cursors.DictCursor
        )

        if connection.open:
            print("Connected to MySQL database")

        # 创建游标对象
        with connection.cursor() as cursor:
            # 执行建表SQL语句
            cursor.execute(create_table_query)

        # 执行commit操作，确保数据写入数据库
        connection.commit()

        print("Table created successfully.")
        response["success"] = 1
        response["message"] = "Table created successfully."

    except pymysql.Error as err:
        print("Error:", err)
        response["message"] = f"Error: {err}"

    # finally:
    #     # 关闭连接
    #     if connection is not None:
    #         connection.close()

    # 将response转换为JSON格式的字符串
    response_json = json.dumps(response)
    return response_json



def drop_region_table(host, user, password, database, drop_table_query):

    response = {"success": 0, "message": ""}

    try:
        # 连接到数据库
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            cursorclass=pymysql.cursors.DictCursor
        )

        if connection.open:
            print("Connected to MySQL database")

        # 创建游标对象
        with connection.cursor() as cursor:
            # 执行建表SQL语句
            cursor.execute(drop_table_query)

        # 执行commit操作，确保数据写入数据库
        connection.commit()

        print("Table dropped successfully.")
        response["success"] = 2
        response["message"] = "Table dropped successfully."

    except pymysql.Error as err:
        print("Error:", err)
        response["message"] = f"Error: {err}"

    finally:
        # 关闭连接
        connection.close()

    # 将response转换为JSON格式的字符串
    response_json = json.dumps(response)
    return response_json  

def alter_region_table(host, user, password, database, alter_table_query):

    response = {"success": 0, "message": ""}

    try:
        # 连接到数据库
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            cursorclass=pymysql.cursors.DictCursor
        )

        if connection.open:
            print("Connected to MySQL database")

        # 创建游标对象
        with connection.cursor() as cursor:
            # 执行建表SQL语句
            cursor.execute(alter_table_query)

        # 执行commit操作，确保数据写入数据库
        connection.commit()

        print("Table altered successfully.")
        response["success"] = 3
        response["message"] = "Table altered successfully."

    except pymysql.Error as err:
        print("Error:", err)
        response["message"] = f"Error: {err}"

    finally:
        # 关闭连接
        connection.close()

    # 将response转换为JSON格式的字符串
    response_json = json.dumps(response)
    return response_json  
   

def query_region_table(host, user, password, database, query):

    response = {"success": 0, "message": "", "data": []}

    try:
        # 连接到数据库
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            cursorclass=pymysql.cursors.DictCursor
        )

        if connection.open:
            print("Connected to MySQL database")

        # 创建游标对象
        with connection.cursor() as cursor:
            # 执行查询表的SQL语句
            cursor.execute(query)
            column_names = [desc[0] for desc in cursor.description]

            # 获取查询结果
            rows = cursor.fetchall()

            # 打印查询结果
            for row in rows:
                row_data = {}
                for column_name in column_names:
                    row_data[column_name] = row[column_name]
                response["data"].append(row_data)

        response["success"] = 4

    except pymysql.Error as err:
        print("Error:", err)
        response["message"] = f"Error: {err}"

    finally:
        # 关闭连接
        connection.close()

    # 将response转换为JSON格式的字符串
    response_json = json.dumps(response)
    return response_json 


 

