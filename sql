CREATE TABLE IF NOT EXISTS customer_churn(
   CustomerID VARCHAR(255),
   City VARCHAR(255),
   Zip_Code INTEGER,
   Gender VARCHAR(255), 
   Senior_Citizen VARCHAR(255),
   Partner VARCHAR(255),
   Dependents VARCHAR(255), 
   Tenure_Months INTEGER,
   Phone_Service VARCHAR(255),
   Multiple_Lines VARCHAR(255),
   Internet_Service VARCHAR(255),
   Online_Security VARCHAR(255),
   Online_Backup VARCHAR(255), 
   Device_Protection VARCHAR(255),
   Tech_Support VARCHAR(255),
   Streaming_TV VARCHAR(255),
   Streaming_Movies VARCHAR(255),
   Contract VARCHAR(255),
   Paperless_Billing VARCHAR(255),
   Payment_Method VARCHAR(255),
   monthly_charges FLOAT,
   Total_Charges FLOAT,
   Churn_Label VARCHAR(255),
   Churn_Value INTEGER,
   Churn_Score INTEGER,
   Churn_Reason TEXT
)

-- Fact Table
CREATE TABLE FactCustomerChurn (
    CustomerID VARCHAR(20),
    TenureMonths INT,
    MonthlyCharges FLOAT,
    TotalCharges FLOAT,
    ChurnLabel BOOLEAN,
    ChurnValue INT,
    ChurnScore INT,
    CLTV INT,
    ChurnReason VARCHAR(255),
    LocationID INT,
    ServiceID INT,
    ContractID INT,
    PRIMARY KEY (CustomerID)
);

-- Dimension Tables
CREATE TABLE DimCustomer (
    CustomerID VARCHAR(20),
    Gender VARCHAR(10),
    SeniorCitizen BOOLEAN,
    Partner BOOLEAN,
    Dependents BOOLEAN,
    PRIMARY KEY (CustomerID)
);

CREATE TABLE DimLocation (
    LocationID INT IDENTITY(1,1),
    Country VARCHAR(50),
    State VARCHAR(50),
    City VARCHAR(50),
    ZipCode VARCHAR(10),
    Latitude FLOAT,
    Longitude FLOAT,
    PRIMARY KEY (LocationID)
);

CREATE TABLE DimService (
    ServiceID INT IDENTITY(1,1),
    PhoneService BOOLEAN,
    MultipleLines BOOLEAN,
    InternetService VARCHAR(20),
    OnlineSecurity BOOLEAN,
    OnlineBackup BOOLEAN,
    DeviceProtection BOOLEAN,
    TechSupport BOOLEAN,
    StreamingTV BOOLEAN,
    StreamingMovies BOOLEAN,
    PRIMARY KEY (ServiceID)
);

CREATE TABLE DimContract (
    ContractID INT IDENTITY(1,1),
    Contract VARCHAR(20),
    PaperlessBilling BOOLEAN,
    PaymentMethod VARCHAR(50),
    PRIMARY KEY (ContractID)
);
