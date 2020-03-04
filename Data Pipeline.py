import sys 
import pandas as pd 
import sqlite3 as sql
from sqlite3 import Error
import datetime
import matplotlib.pyplot as plt 

class DataPipeLine:
    def __init__(self, startDateString, endDateString):
        self.__startDateString = startDateString
        self.__endDateString = endDateString

    def fileNames(self): 
        if len(sys.argv) != 3:
            print(len(sys.argv))
            print("please provide start date and end date in the format mm-dd-yyyy")
            quit()

        try:
            myStartDate = datetime.datetime.strptime(self.__startDateString,"%m-%d-%Y")
        except ValueError as e:
            print(e)
            print("Please Enter proper StartDate in mm-dd-yyyy format")
            quit()

        try:
            myEndDate = datetime.datetime.strptime(self.__endDateString,"%m-%d-%Y")
        except ValueError as e:
            print(e)
            print("Please Enter proper EndDate in mm-dd-yyyy format")
            quit()

        print(myStartDate)
        print(myEndDate)

        if myEndDate < myStartDate: 
            print("End date have to be after start date!")
            quit() 

        fileNameBase = "-SalesData.csv"

        filenameList = []

        d = myStartDate
        delta = datetime.timedelta(days=1)
        
        while d <= myEndDate:
            name = d.strftime("%Y-%m-%d") +fileNameBase
            filenameList.append(name)
            d += delta

        return filenameList

class Database: 
    @staticmethod
    def setConnection(dbName):
        try: 
            conn = sql.connect(dbName)
            return conn
        except Error as e:
            return e
    @staticmethod
    def closeConnection(conn): 
        try:
            conn.close()
        except Error as e:
            return e
class Sales:
    def __init__(self, custID, date, item, revenue):
        self.__custID = custID
        self.__date = date
        self.__item = item
        self.__revenue = revenue
    @property
    def custID(self):
        return self.__custID
    @property
    def date(self):
        return self.__date
    @property
    def item(self):
        return self.__item
    @property
    def revenue(self):
        return self.__revenue
    
    @custID.setter
    def custID(self, custID):
        self.__custID = custID 
    @date.setter
    def date(self, date):
        self.__date= date 
    @item.setter
    def item(self, item):
        self.__item = item 
    @revenue.setter
    def revenue(self, revenue):
        self.__revenue = revenue 
    
    def __str__(self):
        return "{1},{2},{3},{4}".format(self.__custID, self.__date, self.__item, self.__revenue)
    
    @staticmethod
    def addNewSale(conn, newSale):
        #Assume correct format provided
        custID = newSale.custID
        date = newSale.date
        item = newSale.item
        revenue = newSale.revenue
        findCommand = "SELECT * FROM sales WHERE cust_id = ? AND purchase_date = ? AND item = ? AND revenue = ?"
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO sales (cust_id, purchase_date, item, revenue) VALUES (?,?,?,?)", (newSale.custID, newSale.date, newSale.item, newSale.revenue))
            conn.commit()

            #Check if new row is inserted in table
            cursor.execute(findCommand, (custID, date, item, revenue))
            checkData = cursor.fetchall()
            print("New row inserted (see below).")
            return checkData
        except Error as e:
            return e


class Customers:
    def __init__(self, name, sex, age):
        self.__name = name
        self.__sex = sex
        self.__age = age 
        self.__id = 0
    @property
    def name(self):
        return self.__name
    @property
    def sex(self):
        return self.__sex
    @property
    def age(self):
        return self.__age
    @property
    def id(self):
        return self.__id
    
    @name.setter
    def name(self, name):
        self.__name = name 
    @sex.setter
    def sex(self, sex):
        self.__sex= sex 
    @age.setter
    def age(self, age):
        self.__age = age 
    @id.setter
    def id(self, id):
        self.__id = id 

    def __str__(self):
        return "{1},{2},{3},{4}".format(self.__id, self.__name, self.__sex, self.__age)

    @staticmethod
    def addNewCustomer(conn, newCustomer):
        #Assume correct format provided
        name = newCustomer.name 
        sex = newCustomer.sex
        age = newCustomer.age

        findCommand = "SELECT * FROM customers WHERE name = ? AND sex = ? AND age = ?"

        #Find if there is such data in the table 
        try: 
            cursor = conn.cursor()
            cursor.execute(findCommand, (name, sex, age))
            existName = cursor.fetchall()

            if len(existName) == 0: 
                print("No such data, program will initiate a new row")
                cursor.execute("SELECT MAX(id) FROM customers")
                maxID = cursor.fetchall()
                newID = 0 
                for maxid in maxID: #select value in tuple 
                    newID = maxid[0]
                    newID = newID + 1 #assign new id as 1 unit greater than old maximum id
                newCustomer.id = newID
                
                cursor.execute("INSERT INTO customers (id, name, sex, age) VALUES (?,?,?,?)", (newCustomer.id, newCustomer.name, newCustomer.sex, newCustomer.age))
                conn.commit()

                #Check if new row is inserted in table
                cursor.execute(findCommand, (name, sex, age))
                checkData = cursor.fetchall()
                print("New row inserted (see below).")
                return checkData
                
            else: 
                return "This name existed in our file: " + str(existName)

        except Error as e: 
            return e


class DataManager: 
    @staticmethod
    def makeCustomerTable(conn):
        #Create cursor and Make table 
        cursor = conn.cursor()
        cursor.execute(""" CREATE TABLE IF NOT EXISTS customers(
                            id INTEGER PRIMARY KEY,
                            name TEXT, 
                            sex TEXT, 
                            age INTEGER)
                        """)
        conn.commit()
        print("Finish making table customers.")

    @staticmethod
    def makeSalesTable(conn):
        #Create cursor and Make table 
        cursor = conn.cursor()
        cursor.execute(""" CREATE TABLE IF NOT EXISTS sales(
                            cust_id INTEGER,
                            purchase_date SMALLDATETIME, 
                            item TEXT, 
                            revenue INTEGER)
                        """)
        conn.commit()
        print("Finish making table sales.")

    @staticmethod
    def insertCustomerData(conn, dataSet):
        #Read data set 
        try:
            file = pd.read_csv(dataSet, delimiter =",")
        except FileNotFoundError as e: 
            print(e)
            print("Please enter correct name of your dataset")
            quit()

        sqlCommand = "INSERT or IGNORE INTO customers (id, name, sex, age) VALUES (?,?,?,?)"

        try:
            cursor = conn.cursor()
            for index, row in file.iterrows():
                cursor.execute(sqlCommand, (row[0], row[1], row[2], row[3]))
            conn.commit()
            print("Finish importing customer data.")
            print(cursor.lastrowid)
        except Error as e: 
            print(e)

    @staticmethod
    def insertSalesData(conn, dataSet):

        deleteCommand = "DELETE FROM sales"
        try: 
            cursor = conn.cursor()
            cursor.execute(deleteCommand)
            conn.commit()
        except Error as e: 
            print(e)
        
        csvList = []
        #Read data set 
        try:
            for dayFile in dataSet:
                file = pd.read_csv(dayFile, delimiter =",")
                file['Total Amount'] = file['Total Amount'].str.replace('$', '')
                file['Total Amount'] = file['Total Amount'].astype(int)
                csvList.append(file)
        except FileNotFoundError:  #To execute even when there are not enough files. 
            pass   

        sqlCommand = "INSERT or IGNORE INTO sales (cust_id, purchase_date, item, revenue) VALUES (?,?,?,?)"

        try:
            cursor = conn.cursor()
            for file in csvList:
                for index, row in file.iterrows():
                    cursor.execute(sqlCommand, (row[0], row[1], row[2], row[3]))
                conn.commit()
                print("Finish importing sales data.")
                print(cursor.lastrowid)
        except Error as e: 
            print(e)

    @staticmethod
    def analysis(conn):
        #Customer df 
        query1 = "SELECT * FROM customers"

        query2 = "SELECT * FROM sales"

        customers_df = pd.read_sql_query(query1,conn) #Keep young customers in dataset (customer age <18)
        sales_df = pd.read_sql_query(query2,conn)
        
        #Make joined table
        bigdf = pd.merge(sales_df, customers_df, how='left', left_on='cust_id', right_on='id')
        bigdf = bigdf.drop(columns = ["id","sex","age"])

        #Question 1: Identify the Top 5 customers based on $ spent
        top5cust = bigdf.drop(columns = ["purchase_date", "item", "cust_id"])
        top5cust = top5cust.groupby(["name"]).sum().head(5)

        print("Top 5 customers with greatest spendings are (see below).")
        print(top5cust)

        #Question 2: Identify the Top 3 products being sold
        top3product = bigdf["item"].value_counts().head(3)
        print("Top 3 best-selling products are (see below).")
        print(top3product)

        #Question 3: Daily trend of sales per products (data and graph)
        product_dailysale = bigdf[['item', 'purchase_date', "revenue"]]
        product_dailysale = product_dailysale.groupby(['item', 'purchase_date'])['revenue'].sum().reset_index(name = "revenue")

        print(product_dailysale)

        desktop = product_dailysale[product_dailysale['item'] == 'Desktop']
        laptop = product_dailysale.loc[product_dailysale['item'] == 'Laptop']

        desktop = desktop.pivot('purchase_date', 'item')
        laptop = laptop.pivot('purchase_date', 'item')

        plt.plot(desktop, label = "desktop")
        plt.plot(laptop, label = "laptop")

        plt.legend(loc='center left')
        plt.xlabel("Dates")
        plt.xlabel("Sales")
        plt.title("Daily trend of sales per products")
        
        plt.show()
        
        #Quesion 4: Daily trend of sales per customer (data and graph) 
        customer_dailysale = bigdf[['name', 'purchase_date', 'revenue']]
        nameList = customer_dailysale['name'].unique()
        print(nameList)

        customer_dailysale = customer_dailysale.groupby(['name', 'purchase_date'])['revenue'].sum().reset_index(name = "revenue")

        print(customer_dailysale)

        for name in nameList:
            name = customer_dailysale.loc[customer_dailysale['name']==name]
            name = name.pivot('purchase_date', 'name')
            plt.plot(name, label = str(name))

        plt.xlabel("Date")
        plt.xlabel("Sales")
        plt.title("Daily trend of sales per customer")
        plt.show()


        #Question 5: Average sales per day by product (qty) (data and Graph)
        product_dailysale_mean = bigdf[['item', 'purchase_date']]
        product_dailysale_mean = product_dailysale_mean.groupby(['item', 'purchase_date']).size().reset_index(name='count')
        dateList = product_dailysale_mean['purchase_date'].unique()

        product_dailysale_mean['count'] = product_dailysale_mean['count']/len(dateList)

        print(product_dailysale_mean)

        product_dailysale_mean.pivot('item', 'purchase_date').plot(kind = "bar")
        plt.xlabel("Item")
        plt.xlabel("Quantity")
        plt.title("Average sales per day by product(quantity)")

        plt.show()

        #Question 6: Average sales per day by $(data and Graph)
        product_daily_revenue_mean = bigdf[['revenue', 'purchase_date']]
        product_daily_revenue_mean = product_daily_revenue_mean.groupby(['purchase_date']).mean()

        print(product_daily_revenue_mean)

        product_daily_revenue_mean.plot(kind='bar')
        plt.xlabel("Dates")
        plt.xlabel("Sales")
        plt.title("Average sales per day (revenue)")
        plt.show()

        # #Question 7: Average sales per day by customer on $ spent(data and Graph)
        product_daily_revenue_mean_bycust = bigdf[['name', 'revenue', 'purchase_date']]
        product_daily_revenue_mean_bycust = product_daily_revenue_mean_bycust.groupby(['name','purchase_date'])['revenue'].mean()
        print(product_daily_revenue_mean_bycust)
        
        product_daily_revenue_mean_bycust.unstack(level=-1).plot(kind = "bar")

        plt.xlabel('Customer')
        plt.ylabel('Average Revenue')
        plt.title("Average revenue per day by customer")
        plt.show()

def Main():
    dbName = "sales.db"
    customerData = "CustomerData.csv"
    conn = Database.setConnection(dbName)

    ###Filename
    pipeline = DataPipeLine(sys.argv[1], sys.argv[2])
    salesFileList = pipeline.fileNames()
    print(salesFileList)

    ###Make tables
    DataManager.makeCustomerTable(conn)
    DataManager.makeSalesTable(conn)

    ###Insert Data 
    DataManager.insertCustomerData(conn, customerData)
    DataManager.insertSalesData(conn,salesFileList)

    ###Use Pandas for Analysis
    DataManager.analysis(conn)

    ###New Customer
    cust1 = Customers("Hannah","female","24")
    print(Customers.addNewCustomer(conn, cust1))

    ###New Sale
    sale1 = Sales('11', "10/3/2017", "Table", "20")
    print(Sales.addNewSale(conn, sale1))
    
    Database.closeConnection(conn)

if __name__ == "__main__":
    Main()
