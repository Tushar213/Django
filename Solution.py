import psycopg2

def calculate_savings(events):
    # establishing the connection
    database = events["database"]
    port = events["port"]
    host = events["host"]
    user = events["user"] 
    password = events["password"]
    date = "-".join(events["date"].split("/")[::-1]) # convert date from dd/mm/yyyy to yyyy-mm-dd
    try:
        conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port= port
        )

        #CREATE cursor object
        cursor = conn.cursor()

        #query to get the age and avg savings
        # 1) we get Customer ID,txn type,txn amount from transactions table if the transaction_date matches the events date
        # 2) we get total(txn_amount is set as positive or negative based on the txn_type ie. positive for credit and negative for debit)
        # 3) we get savings(sum of total) for each unique Customer ID
        # 4) we get date of birth from customer table if both foreign key and primary key match
        # 5) we get age from DOB 
        # 6) we get average of savings for each unique age
        sql = """select t.age,CAST (AVG(t.total) AS INTEGER) from
          (select t.total, date_part('year',age(DOB)) :: int as age from 
            (select DATE(c.date_of_birth) AS DOB, t.customer_id, t.total from
              (select t.customer_id, SUM(t.total) as total from
                (select t.customer_id, CASE when t.txn_type = 'credit' then t.txn_amount else (t.txn_amount*-1) END total from
                  (select customer_id,txn_type,txn_amount from Transactions where DATE(transaction_date) = %s) as t)
                    as t group by t.customer_id) as t FULL JOIN Customer as c on t.customer_id = c.customer_id ) as t) 
                      as t Group by t.age"""

        # execute the query 
        cursor.execute(sql,(date,))
        
        # fetch the output and store it in val
        val = cursor.fetchall()
        data = {}
        for x in val:
            data[x[0]] = x[1]
    except Exception as e:
        return {"statusCode": 400, "message": 'An exception occurred: {}'.format(e)}
    
    return {"statusCode": 200, "data": data}

#Main Function
if __name__ == "__main__":
    events = {}
    events["database"] = "postgres"
    events["port"] = "5433"
    events["host"] = "127.0.0.1"
    events["user"] = "postgres"
    events["password"] = "password"
    events["date"] = "02/12/2022"
    print (calculate_savings(events))
    

