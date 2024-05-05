import mysql.connector
import json

def create_region_table(host, user, password, database, create_table_query):

    response = {"success": False, "message": ""}

    try:
        # 连接到数据库
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print("Connected to MySQL database")

        # 创建游标对象
        cursor = connection.cursor()

        # 执行建表SQL语句
        cursor.execute(create_table_query)

        print("Table created successfully.")
        response["success"] = True
        response["message"] = "Table created successfully."

    except mysql.connector.Error as err:
        print("Error:", err)
        response["message"] = f"Error: {err}"

    finally:
        # 关闭游标和连接
        cursor.close()
        connection.close()

    # 将response转换为JSON格式的字符串
    response_json = json.dumps(response)
    return response_json

def drop_region_table(host, user, password, database, drop_table_query):

    response = {"success": False, "message": ""}

    try:
        # 连接到数据库
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print("Connected to MySQL database")

        # 创建游标对象
        cursor = connection.cursor()

        # 执行删除表SQL语句
        cursor.execute(drop_table_query)

        print("Table dropped successfully.")
        response["success"] = True
        response["message"] = "Table dropped successfully."   

    except mysql.connector.Error as err:
        print("Error:", err)
        response["message"] = f"Error: {err}"        

    finally:
        # 关闭游标和连接
        cursor.close()
        connection.close()

    # 将response转换为JSON格式的字符串
    response_json = json.dumps(response)
    return response_json    

def alter_region_table(host, user, password, database, alter_table_query):

    response = {"success": False, "message": ""}

    try:
        # 连接到数据库
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print("Connected to MySQL database")

        # 创建游标对象
        cursor = connection.cursor()

        # 执行修改表SQL语句
        cursor.execute(alter_table_query)

        print("Table altered successfully.")
        response["success"] = True
        response["message"] = "Table altered successfully."           


    except mysql.connector.Error as err:
        print("Error:", err)
        response["message"] = f"Error: {err}"   

    finally:
        # 关闭游标和连接
        cursor.close()
        connection.close()

    # 将response转换为JSON格式的字符串
    response_json = json.dumps(response)
    return response_json   
   

def query_region_table(host, user, password, database, query):

    response = {"success": False, "message": "", "data": []}

    try:
        # 连接到数据库
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print("Connected to MySQL database")

        # 创建游标对象
        cursor = connection.cursor()

        # 执行查询表的SQL语句
        cursor.execute(query)
        column_names = [desc[0] for desc in cursor.description]

        # 获取查询结果
        rows = cursor.fetchall()

        # 打印查询结果
        for row in rows:
            #print(row)
            row_data = {}
            for i in range(len(column_names)):
                row_data[column_names[i]] = row[i]
            response["data"].append(row_data)

        # return rows

        response["success"] = True



    except mysql.connector.Error as err:
        print("Error:", err)
        response["message"] = f"Error: {err}" 

    finally:
        # 关闭游标和连接
        cursor.close()
        connection.close()
        
    # 将response转换为JSON格式的字符串
    response_json = json.dumps(response)
    return response_json   


 

