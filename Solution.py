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
    

