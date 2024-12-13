**Sell In Data Flow**
![image](https://github.com/user-attachments/assets/741923b2-0f23-43c1-8ff5-b1136315cc93)

**Python Description**
1. bosnet_po_tracking 
This Python Take data from Bosnet (SQL Server) to Data Warehouse (Google Big Query) for PO MTD data
Logic:
- PO date = MTD
- Channel = 'GT', 'MTI'
- Status = Excluding 'Rejected', 'Canceled'
- PO Number = Excluding 'RES'

2. bosnet_po_tracking_v1a
This Python Take data from Bosnet (SQL Server) to Data Warehouse (Google Big Query) for PO MTD data
Logic:
- PO date = Last 3 Month inclued MTD
- Channel = 'GT', 'MTI'
- Status = Excluding 'Rejected', 'Canceled'

**SI Data Migration Flow**
This flow only run after month closing 
![image](https://github.com/user-attachments/assets/78896d40-345c-49ba-a260-6abdb4334e73)

**Data Lineage on Data Warehouse**
1.Shipment
![image](https://github.com/user-attachments/assets/4dc5e2ab-5220-43bc-a824-222683ced285)

2.PO
![image](https://github.com/user-attachments/assets/e02f1e93-ba55-422b-a7a7-90ef656d8189)


_In this repository you can download the python, sql from sql server to, and sql from data warehouse_
