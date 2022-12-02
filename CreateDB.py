import psycopg2

# establishing the connection
conn = psycopg2.connect(
   database="postgres", user='postgres', password='password', host='127.0.0.1', port= '5433'
)
# # conn.autocommit = True

# # #Creating a cursor object using the cursor() method
cursor = conn.cursor()

# #Preparing query to create a database
# sql = '''CREATE database MoneyClub''';

# #Creating a database
# cursor.execute(sql)
# print("Database created successfully........")

# cursor.execute("ALTER TABLE Transactions RENAME COLUMN date_of_birth TO transaction_date")

# #Creating table as per requirement
# sql ='''CREATE TABLE Transactions(
#    txn_id INT,
#    txn_type VARCHAR(10),
#    tsx_amount INT,
#    transaction_date timestamptz,
#    customer_id INT references Customer (customer_id)
# )'''
# cursor.execute(sql)
# print("Table created successfully........")

# sql = "select * from Customer"
# sql ='''INSERT INTO Customer (customer_id, first_name, last_name, date_of_birth) 
# VALUES('1','Tushar','Trivedi','1998-08-01 14:01:10')'''
# cursor.execute(sql)
# sql ='''INSERT INTO Customer
# VALUES ('2', 'Aryan', 'Trivedi', '2000-01-01 14:01:10'
#  )'''
# cursor.execute(sql)
# sql ='''INSERT INTO Customer
# VALUES ('3', 'Aryan', 'Bajpai', '2000-02-01 14:01:10'
#  )'''
# cursor.execute(sql)

# sql ='''INSERT INTO Transactions
# VALUES ('10', 'credit', '20', '2022-12-02 14:01:10','1'
#  )'''
# cursor.execute(sql)
# sql ='''INSERT INTO Transactions
# VALUES ('11', 'credit', '5', '2022-12-02 14:01:10','1'
#  )'''
# cursor.execute(sql)
# sql ='''INSERT INTO Transactions
# VALUES ('12', 'credit', '10', '2022-12-02 14:01:10','1'
#  )'''
# cursor.execute(sql)
# sql ='''INSERT INTO Transactions
# VALUES ('13', 'debit', '15', '2022-12-02 14:01:10','1'
#  )'''
# cursor.execute(sql)
# sql ='''INSERT INTO Transactions
# VALUES ('14', 'credit', '20', '2022-12-02 14:01:10','1'
#  )'''
# cursor.execute(sql)
# sql ='''INSERT INTO Transactions
# VALUES ('15', 'debit', '10', '2022-12-01 14:01:10','1'
#  )'''
# sql = "select txn_type from Transactions"
# cursor.execute(sql)

# val = cursor.fetchall()
# print (val)

conn.commit()

#Closing the connection
conn.close()