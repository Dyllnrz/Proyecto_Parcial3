CREATE TABLE IF NOT EXISTS dim_Category (
    Category_Id  INTEGER        PRIMARY KEY,
    CategoryName VARCHAR (8000),
    Description  VARCHAR (8000) 
);

CREATE TABLE IF NOT EXISTS dim_Customer (
    Customer_Id  VARCHAR (8000) PRIMARY KEY,
    CompanyName  VARCHAR (8000),
    ContactName  VARCHAR (8000),
    ContactTitle VARCHAR (8000),
    Address      VARCHAR (8000),
    City         VARCHAR (8000),
    Region       VARCHAR (8000),
    PostalCode   VARCHAR (8000),
    Country      VARCHAR (8000),
    Phone        VARCHAR (8000) 
);

CREATE TABLE IF NOT EXISTS dim_Employee (
    Employee_Id     INTEGER        PRIMARY KEY,
    LastName        VARCHAR (8000),
    FirstName       VARCHAR (8000),
    Title           VARCHAR (8000),
    TitleOfCourtesy VARCHAR (8000),
    BirthDate       VARCHAR (8000),
    HireDate        VARCHAR (8000),
    Address         VARCHAR (8000),
    City            VARCHAR (8000),
    Region          VARCHAR (8000),
    PostalCode      VARCHAR (8000),
    Country         VARCHAR (8000),
    Extension       VARCHAR (8000),
    Notes           VARCHAR (8000),
    PhotoPath       VARCHAR (8000) 
);


CREATE TABLE IF NOT EXISTS dim_EmployeeTerritory (
    EmployeeTerritory_Id VARCHAR (8000) PRIMARY KEY,
    Territory_Id         INTEGER        REFERENCES dim_Territory
);

CREATE TABLE IF NOT EXISTS dim_Order (
    Order_Id       INTEGER        PRIMARY KEY,
    OrderDate      VARCHAR (8000),
    RequiredDate   VARCHAR (8000),
    ShippedDate    VARCHAR (8000),
    ShipVia        INTEGER        REFERENCES dim_Shipper (Shipper_Id),
    Freight        DECIMAL        NOT NULL,
    ShipName       VARCHAR (8000),
    ShipAddress    VARCHAR (8000),
    ShipCity       VARCHAR (8000),
    ShipRegion     VARCHAR (8000),
    ShipPostalCode VARCHAR (8000),
    ShipCountry    VARCHAR (8000) 
);

CREATE TABLE IF NOT EXISTS dim_Product (
    Product_Id      INTEGER        PRIMARY KEY,
    Supplier_Id     INTEGER        REFERENCES Suplier (Supplier_Id),
    Category_Id     INTEGER        REFERENCES Suplier (Category_Id),
    ProductName     VARCHAR (8000),
    QuantityPerUnit VARCHAR (8000),
    UnitPrice       DECIMAL        NOT NULL,
    UnitsInStock    INTEGER        NOT NULL,
    UnitsOnOrder    INTEGER        NOT NULL,
    ReorderLevel    INTEGER        NOT NULL,
    Discontinued    INTEGER        NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_Region (
    Region_Id         INTEGER        PRIMARY KEY,
    RegionDescription VARCHAR (8000) 
);

CREATE TABLE IF NOT EXISTS dim_Shipper (
    Shipper_Id  INTEGER        PRIMARY KEY,
    CompanyName VARCHAR (8000),
    Phone       VARCHAR (8000) 
);

CREATE TABLE IF NOT EXISTS dim_Supplier (
    Supplier_Id  INTEGER        PRIMARY KEY,
    CompanyName  VARCHAR (8000),
    ContactName  VARCHAR (8000),
    ContactTitle VARCHAR (8000),
    Address      VARCHAR (8000),
    City         VARCHAR (8000),
    Region       VARCHAR (8000),
    PostalCode   VARCHAR (8000),
    Country      VARCHAR (8000),
    Phone        VARCHAR (8000) 
);

CREATE TABLE IF NOT EXISTS dim_Territory (
    Territory_Id         VARCHAR (8000) PRIMARY KEY,
    TerritoryDescription VARCHAR (8000),
    Region_Id            INTEGER        REFERENCES dim_Region (Region_Id) 
);

CREATE TABLE IF NOT EXISTS OrderDetail (
    ODetail_Id  VARCHAR (8000) PRIMARY KEY,
    Order_Id    INTEGER        REFERENCES dim_Order,
    Customer_Id INTEGER        REFERENCES dim_Customer,
    Employee_Id INTEGER        REFERENCES dim_Employee,
    Product_Id  INTEGER        REFERENCES dim_Product (Product_Id),
    UnitPrice   DECIMAL        NOT NULL,
    Quantity    INTEGER        NOT NULL,
    Discount    DOUBLE         NOT NULL
);
