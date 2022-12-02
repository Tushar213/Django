import psycopg2


# establishing the connection
conn = psycopg2.connect(
   database="postgres", user='postgres', password='password', host='127.0.0.1', port= '5433'
)

#CREATE cursor object
cursor = conn.cursor()

#query to get the age and avg savings
sql = """select t.age,CAST (AVG(t.total) AS INTEGER) from
(select t.total, date_part('year',age(DOB)) :: int as age from 
(select DATE(c.date_of_birth) AS DOB, t.customer_id, t.total from
(select t.customer_id, SUM(t.total) as total from
(select t.customer_id, CASE when t.txn_type = 'credit' then t.txn_amount else (t.txn_amount*-1) END total from
(select customer_id,txn_type,txn_amount from Transactions where DATE(transaction_date) = %s) as t) as t group by t.customer_id)
as t FULL JOIN Customer as c on t.customer_id = c.customer_id ) as t) as t Group by t.age"""

# sql = """select * from Transactions"""
cursor.execute(sql,('2022-12-02',))

val = cursor.fetchall()
print (val)
