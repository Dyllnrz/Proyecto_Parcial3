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

def transformacion_ship(connDB):

    query = """SELECT Id as Shipper_Id, CompanyName, Phone  FROM Shipper;"""
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformacion_prod(connDB):

    query = """SELECT product.Id as Product_Id, ProductName, QuantityPerUnit, UnitPrice, UnitsInStock,
                UnitsOnOrder, ReorderLevel, Discontinued, Category.CategoryName, Category.Description
                FROM Product
                Inner Join Category on Product.CategoryId = Category.Id;                
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
         Product.Id as Product_Id, Shipper.Id as Shipper_Id, 
         Supplier.Id as Supplier_Id, OrderDetail.UnitPrice, OrderDetail.Quantity, OrderDetail.Discount  
         From [OrderDetail]
    Inner Join [Order] on OrderDetail.OrderId = [Order].Id
    Inner Join Customer on [Order].CustomerId = Customer.Id
    Inner Join Employee on [Order].EmployeeId = Employee.Id
    Inner Join Shipper on [Order].ShipVia = Shipper.Id
    Inner Join Product on OrderDetail.ProductId = Product.Id
    Inner Join Supplier on Product.SupplierId = Supplier.Id
    """
    result = connDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformacion_employee(connDB):

    query = """SELECT Employee.Id as Employee_Id, Employee.LastName, Employee.FirstName, Employee.Title, Employee.TitleOfCourtesy, Employee.BirthDate,
                Employee.HireDate, Employee.Address, Employee.City, Employee.Region, Employee.PostalCode, Employee.Country,
                Employee.Extension, Employee.Notes, Employee.PhotoPath, Region.RegionDescription, Territory.TerritoryDescription
            FROM EmployeeTerritory
    Inner Join Employee on EmployeeTerritory.EmployeeId = Employee.Id
    Inner Join Territory on EmployeeTerritory.TerritoryId = Territory.Id
    Inner Join Region on Territory.RegionId = Region.Id
    Group By 1
    ; """
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
    direccion2 = "sqlite:///Northwind_estrella.db"

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
    
    #Definición de tabla
    tabla1 = "dim_Shipper"
    tabla2 = "dim_Product"
    tabla3 = "dim_Supplier"
    tabla4 = "dim_Order"
    tabla5 = "dim_Customer"
    tabla6 = "OrderDetail"
    tabla7 = "dim_Employee"
    
    #Proceso de carga
    load(extraer1, connex, tabla1)
    #load(extraer2, connex, tabla2)  
    #load(extraer3, connex, tabla3)
    #load(extraer4, connex, tabla4)
    #load(extraer5, connex, tabla5)
    #load(extraer6, connex, tabla6)
    #load(extraer7, connex, tabla7)

    print("ETL completada")