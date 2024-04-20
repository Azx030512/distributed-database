import mysql.connector

def create_region_table(host, user, password, database, create_table_query):
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

    except mysql.connector.Error as err:
        print("Error:", err)

    finally:
        # 关闭游标和连接
        cursor.close()
        connection.close()

def drop_region_table(host, user, password, database, drop_table_query):
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

    except mysql.connector.Error as err:
        print("Error:", err)

    finally:
        # 关闭游标和连接
        cursor.close()
        connection.close()

def alter_region_table(host, user, password, database, alter_table_query):
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

    except mysql.connector.Error as err:
        print("Error:", err)

    finally:
        # 关闭游标和连接
        cursor.close()
        connection.close()

def query_region_table(host, user, password, database, query):
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

        # 获取查询结果
        rows = cursor.fetchall()

        # 打印查询结果
        for row in rows:
            print(row)

        return rows



    except mysql.connector.Error as err:
        print("Error:", err)

    finally:
        # 关闭游标和连接
        cursor.close()
        connection.close()


 

