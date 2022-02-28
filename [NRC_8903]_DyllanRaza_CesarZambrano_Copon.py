import pandas as pd
import sqlalchemy

def extract(path):
    #Dyllan Raza

    Databasec = sqlalchemy.create_engine(path)
    conn = Databasec.connect()

    return Databasec, conn

def extract_n(path):
    
    Databasen = sqlalchemy.create_engine(path)
    conn_n = Databasen.connect()

    return Databasen, conn_n


def transformacion_cat(connDB):

    query = """SELECT Id as Category_Id, CategoryName, Description  FROM Category;"""
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformacion_ship(connDB):

    query = """SELECT Id as Shipper_Id, CompanyName, Phone  FROM Shipper;"""
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformacion_prod(connDB):

    query = """SELECT product.Id as Product_Id, SupplierId as Supplier_Id, CategoryId as Category_Id, ProductName, QuantityPerUnit, UnitPrice, UnitsInStock,
                UnitsOnOrder, ReorderLevel, Discontinued
                FROM Product
            """
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformacion_supl(connDB):

    query = """SELECT Id as Supplier_Id, CompanyName, ContactName, ContactTitle, Address,
                City, Region, PostalCode, Country, Phone
                FROM Supplier;"""
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformacion_order(connDB):

    query = """SELECT Id as Order_Id, OrderDate, RequiredDate, ShippedDate, ShipVia,
                Freight, ShipName, ShipAddress, ShipCity, ShipRegion, 
                ShipPostalCode, ShipCountry
                FROM [Order];"""
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformacion_customer(connDB):

    query = """SELECT Id as Customer_Id, CompanyName, ContactName, ContactTitle, Address, City,
                Region, PostalCode, Country, Phone
                FROM Customer;"""
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformacion_odetail(connDB):

    query = """
Select OrderDetail.Id as ODetail_Id, [Order].Id as Order_Id,
         Customer.Id as Customer_Id, Employee.Id as Employee_Id, 
         Product.Id as Product_Id, OrderDetail.UnitPrice, OrderDetail.Quantity, OrderDetail.Discount  
         From [OrderDetail]
    Inner Join [Order] on OrderDetail.OrderId = [Order].Id
    Inner Join Customer on [Order].CustomerId = Customer.Id
    Inner Join Employee on [Order].EmployeeId = Employee.Id
    Inner Join Product on OrderDetail.ProductId = Product.Id;
    """
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformacion_employee(connDB):

    query = """SELECT Employee.Id as Employee_Id, Employee.LastName, Employee.FirstName, Employee.Title, Employee.TitleOfCourtesy, Employee.BirthDate,
                Employee.HireDate, Employee.Address, Employee.City, Employee.Region, Employee.PostalCode, Employee.Country,
                Employee.Extension, Employee.Notes, Employee.PhotoPath
            FROM Employee; """
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformacion_empt(connDB):

    query = """SELECT Id as EmployeeTerritory_Id, TerritoryId as Territory_Id
            FROM EmployeeTerritory; """
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformacion_terr(connDB):

    query = """SELECT Id as Territory_Id, TerritoryDescription, RegionId as Region_Id
            FROM Territory; """
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformacion_reg(connDB):

    query = """SELECT Id as Region_Id, RegionDescription
            FROM Region; """
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def load(datos, connectar, tabla):

    # Ingreso de datos en la tabla correspondiente con su conexión
    datos.to_sql(tabla, connectar, if_exists='append', index=False)
    connectar.close()
    return print("Datos cargados")

if __name__ == '__main__':
    direccion1 = "sqlite:///Northwind_large.sqlite"
    direccion2 = "sqlite:///Northwind_copon.db"

    # Extración de la BD Chinook
    extraerBD = extract(direccion1)
    
    #Definición de la conexión secundaria
    DatabaseEx = extract_n(direccion2)
    connex = DatabaseEx[1]

    #Extracción de cada tabla 
    engine = extraerBD[0]
    extraer1 = transformacion_ship(engine)
    extraer2 = transformacion_prod(engine)
    extraer3 = transformacion_supl(engine)
    extraer4 = transformacion_order(engine)
    extraer5 = transformacion_customer(engine)
    extraer6 = transformacion_odetail(engine)
    extraer7 = transformacion_employee(engine)
    extraer8 = transformacion_cat(engine)
    extraer9 = transformacion_terr(engine)
    extraer10 = transformacion_reg(engine)
    extraer11 = transformacion_empt(engine)
    
    #Definición de tabla
    tabla1 = "dim_Shipper"
    tabla2 = "dim_Product"
    tabla3 = "dim_Supplier"
    tabla4 = "dim_Order"
    tabla5 = "dim_Customer"
    tabla6 = "OrderDetail"
    tabla7 = "dim_Employee"
    tabla8 = "dim_Category"
    tabla9 = "dim_Territory"
    tabla10 = "dim_Region"
    tabla11 = "dim_EmployeeTerritory"
    
    #Proceso de carga
    #load(extraer1, connex, tabla1)
    #load(extraer2, connex, tabla2)  
    #load(extraer3, connex, tabla3)
    #load(extraer4, connex, tabla4)
    #load(extraer5, connex, tabla5)
    #load(extraer6, connex, tabla6)
    #load(extraer7, connex, tabla7)
    #load(extraer8, connex, tabla8)
    #load(extraer9, connex, tabla9)
    #load(extraer10, connex, tabla10)
    load(extraer11, connex, tabla11)

    print("ETL completada")