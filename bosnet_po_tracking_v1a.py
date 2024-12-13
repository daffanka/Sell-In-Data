import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
from concurrent.futures import ThreadPoolExecutor
import numpy as np  # Ensure numpy is imported correctly

# SQL Server connection details
sql_server_connection_str = (
    "mssql+pyodbc://readuser:Read123$%@skintific.database.windows.net/MSYN-PRODUCTION?"
    "driver=ODBC Driver 17 for SQL Server"
)
# SQL query
sql_query = """
SELECT DISTINCT
    CASE  
        WHEN ISNULL(TRIM(so.szCustPoId), '') = '' THEN si.szFPoId
        ELSE ISNULL(TRIM(so.szCustPoId), '') 
    END AS purchase_order_number,
    ISNULL(po.dtmCreated, '1900-01-01') AS date_created,
    ISNULL(so.dtmCreated, '1900-01-01') AS po_on_review_date,
    ISNULL(so.dtmOrder, '1900-01-01') AS po_approve_lv1_date,
    si.szFSoId AS sales_order_number,
    cnclOrd.bAlreadyTransferred AS canceled_order,
    rejected.bRejected AS rejected_order,
    ISNULL(purchOrd.dtmTransferred, '1900-01-01') AS send_to_wms_date,
    si.szCustId AS customer_id,
    cust.szName AS customer_name,
    ISNULL(custCat.szDescription, '') AS customer_category_description,
    ISNULL(delv.outTime, '1900-01-01') AS delivered_date_feedback_wms,
    ISNULL (porec.dtmReceipt, '1900-01-01') AS received_date,
    CASE 
        WHEN si.szFPoId = '' THEN so.dtmCreated 
        ELSE po.dtmCreated 
    END AS poDate,
    CASE 
        WHEN TRIM(ISNULL(si.szFSoId, '')) = '' THEN poi.szProductId 
        ELSE soi.szProductId 
    END AS product_id,
    CASE 
        WHEN TRIM(ISNULL(si.szFSoId, '')) = '' THEN prod.szName 
        ELSE prod1.szName 
    END AS product_name,
		isnull( delvItemJoinSO.qty,0)
 		qty_delivered,
	CASE
		WHEN TRIM(ISNULL(FDN.szFDNId, '')) = '' THEN 0
		ELSE isnull(fdnItem1.decQty,0)
        END as close_dn_qty,
    CASE 
        WHEN TRIM(ISNULL(si.szFSoId, '')) = '' THEN poi.decAmount 
        ELSE soi.decAmount 
    END AS amount_incl_ppn_before_discount,
    CASE 
        WHEN TRIM(ISNULL(si.szFSoId, '')) = '' 
        THEN poi.decAmount - ISNULL(bonus.decDiscFakturDPP, 0) - ISNULL(bonus.decDiscFakturTax, 0)
        ELSE soi.decAmount - ISNULL(bonus.decDiscFakturDPP, 0) - ISNULL(bonus.decDiscFakturTax, 0)
    END AS nett_amount_incl_ppn,
    ISNULL((
        SELECT TOP 1 cdn.bApplied 
        FROM MSYN_INV_CloseDN cdn
        LEFT JOIN MSYN_INV_CloseDNItem cdnItem ON cdn.szCloseDNId = cdnItem.szCloseDNId
        WHERE cdnItem.szDNId = si.szFDnId
        ORDER BY dtmLastUpdated DESC
    ), 0) AS bStatusDN,
    ISNULL(FDN.dtmLastUpdated, '1900-01-01') AS dn_closed_date,
    si.szfdnid,
CONCAT(
    CASE  
        WHEN TRIM(COALESCE(so.szCustPoId, '')) = '' THEN si.szFPoId
        ELSE TRIM(COALESCE(so.szCustPoId, ''))
    END,
    '-', 
    CASE 
        WHEN TRIM(COALESCE(si.szFSoId, '')) = '' THEN poi.szProductId 
        ELSE soi.szProductId
    END
) AS InvoiceProductID

FROM msyn_sd_salesinquiry si
LEFT JOIN bos_sd_fso so ON si.szFSoId = so.szFSoId
LEFT JOIN BOS_PUR_FPo_s po ON si.szFPoId = po.szFPo_sId
LEFT JOIN BOS_SD_FSoItem soi ON soi.szFSoId = so.szFSoId
LEFT JOIN BOS_PUR_FPo_sItem poi ON poi.szFPo_sId = po.szFPo_sId
LEFT JOIN BOS_INV_Product prod ON poi.szProductId = prod.szProductId
LEFT JOIN BOS_INV_Product prod1 ON soi.szProductId = prod1.szProductId
LEFT JOIN bos_ar_customer cust ON cust.szCustId = si.szCustId
LEFT JOIN BOS_PUR_FPoReceipt_s Porec ON Porec.szPoId = po.szFPo_sId AND Porec.szPoId <> ''
LEFT JOIN BOS_PUR_FPoReceipt_sItem PorecI ON PorecI.szFPoReceipt_sId = Porec.szFPoReceipt_sId 
AND PorecI.szProductId = poi.szProductId
LEFT JOIN BOS_PUR_FPoReceipt_sItem PorecI1 ON PorecI1.szFPoReceipt_sId = Porec.szFPoReceipt_sId 
AND PorecI1.szProductId = soi.szProductId
LEFT JOIN BOS_SD_FDo do ON do.szDoId = si.szFDoId
LEFT JOIN MSYN_INT_WMS_DeliveryConfirmation delv ON delv.custOrderNo = si.szFSoId AND delv.custOrderNo <> '' 
AND do.szDoId = delv.szDoBosnetId
LEFT JOIN (
    SELECT custOrderNo, code, orderNo, SUM(qty) qty 
    FROM MSYN_INT_WMS_DeliveryConfirmationItem 
    GROUP BY custOrderNo, code, orderNo
) delvItemJoinSO ON delvItemJoinSO.custOrderNo = delv.custOrderNo 
AND delv.orderNo = delvItemJoinSO.orderNo 
AND delv.custOrderNo <> '' 
AND delvItemJoinSO.code = soi.szProductId
LEFT JOIN BOS_SD_FInvoice invoice ON invoice.szFInvoiceId = do.szFInvoiceId
LEFT JOIN BOS_SD_FInvoiceItem invItem ON invItem.szFInvoiceId = invoice.szFInvoiceId 
AND invItem.szProductId = soi.szProductId 
AND invItem.szOrderItemTypeId = soi.szOrderItemTypeId
LEFT JOIN BOS_AR_Category custCat ON custCat.szCategoryId = cust.szCategory_2
LEFT JOIN MSYN_INT_PurchaseCancelOrder cancelOrd ON cancelOrd.szDocId = si.szFSoId AND cancelOrd.bAlreadyTransferred = 1
LEFT JOIN MSYN_INT_PurchaseOrder purchOrd ON purchOrd.szDocId = si.szFSoId AND purchOrd.bAlreadyTransferred = 1
LEFT JOIN (
    SELECT soItem.szFSoId, bonus.szProductId, bonus.szParentId, bonus.szPaymentType, bonus.szOrderItemTypeId,
        SUM(CASE 
            WHEN soItem.szPrincipalDiscRefId = 'Invoice Disc. Item' THEN 
                CASE 
                    WHEN soItem.bTaxable = 1 AND (soItem.decDPP + soItem.decTax) <> 0
                    THEN bonus.decBonusAmount * soitem.decDPP / (soItem.decDPP + soItem.decTax) 
                    ELSE bonus.decBonusAmount 
                END
            ELSE 0 
        END) AS decDisc1DPP,
        SUM(CASE 
            WHEN soItem.szPrincipalDiscRefId = 'Invoice Disc. Item' THEN 
                CASE 
                    WHEN soItem.bTaxable = 1 AND (soItem.decDPP + soItem.decTax) <> 0 
                    THEN bonus.decBonusAmount * soitem.decTax / (soItem.decDPP + soItem.decTax) 
                    ELSE 0 
                END
            ELSE 0 
        END) AS decDisc1Tax,
        SUM(CASE 
            WHEN soItem.szPrincipalDiscRefId = 'Invoice Disc. Item 2' THEN 
                CASE 
                    WHEN soItem.bTaxable = 1 AND (soItem.decDPP + soItem.decTax) <> 0 
                    THEN bonus.decBonusAmount * soitem.decDPP / (soItem.decDPP + soItem.decTax) 
                    ELSE bonus.decBonusAmount 
                END
            ELSE 0 
        END) AS decDisc2DPP,
        SUM(CASE 
            WHEN soItem.szPrincipalDiscRefId = 'Invoice Disc. Item 2' THEN 
                CASE 
                    WHEN soItem.bTaxable = 1 AND (soItem.decDPP + soItem.decTax) <> 0
                    THEN bonus.decBonusAmount * soitem.decTax / (soItem.decDPP + soItem.decTax) 
                    ELSE bonus.decBonusAmount 
                END
            ELSE 0 
        END) AS decDisc2Tax,
        SUM(CASE 
            WHEN soItem.szPrincipalDiscRefId = 'Invoice Disc.' THEN 
                CASE 
                    WHEN soItem.bTaxable = 1 AND (soItem.decDPP + soItem.decTax) <> 0
                    THEN bonus.decBonusAmount * soitem.decDPP / (soItem.decDPP + soItem.decTax) 
                    ELSE bonus.decBonusAmount 
                END
            ELSE 0 
        END) AS decDiscFakturDPP,
        SUM(CASE 
            WHEN soItem.szPrincipalDiscRefId = 'Invoice Disc.' THEN 
                CASE 
                    WHEN soItem.bTaxable = 1 AND (soItem.decDPP + soItem.decTax) <> 0
                    THEN bonus.decBonusAmount * soitem.decTax / (soItem.decDPP + soItem.decTax) 
                    ELSE bonus.decBonusAmount 
                END
            ELSE 0 
        END) AS decDiscFakturTax
    FROM BOS_SD_FSoItem soItem
    LEFT JOIN bos_sd_fso so ON soitem.szFsoId = so.szFSoId
    LEFT JOIN BOS_SD_FSoItemBonusSource bonus ON bonus.szFSoId = soItem.szFSoId 
    AND soitem.shItemNumber = bonus.shItemNumber
    WHERE bonus.szProductId IS NOT NULL AND so.dtmOrder >= '2024-12-01'
    GROUP BY soItem.szFSoId, bonus.szProductId, bonus.szParentId, bonus.szPaymentType, bonus.szOrderItemTypeId
) bonus ON bonus.szFSoId = soi.szFSoId 
AND soi.szProductId = bonus.szProductId 
AND bonus.szParentId = soi.szParentId
AND bonus.szPaymentType = soi.szPaymentType 
AND bonus.szOrderItemTypeId = soi.szOrderItemTypeId
LEFT JOIN (
    SELECT TOP 1 btApprovedLevel, gdApprovedId 
    FROM BOS_GEN_ApprovedItem BGADitem 
    ORDER BY BGADitem.btApprovedLevel DESC
) approvedlvl ON approvedlvl.gdApprovedId = so.gdApprovedId
LEFT JOIN BOS_GEN_ApprovedItem gd1 ON gd1.gdApprovedId = so.gdApprovedId AND gd1.btApprovedLevel = 1
LEFT JOIN BOS_GEN_ApprovedItem gd2 ON gd2.gdApprovedId = so.gdApprovedId AND gd2.btApprovedLevel = 2
LEFT JOIN BOS_GEN_ApprovedItem gd3 ON gd3.gdApprovedId = so.gdApprovedId AND gd3.btApprovedLevel = 3                                          
LEFT JOIN MSYN_INV_FDn FDN ON FDN.szfdnid = si.szFDNId
LEFT JOIN MSYN_INV_FDnItem fdnItem1 ON FDN.szFDNId = fdnItem1.szFDNId AND fdnItem1.szProductId = soi.szProductId
LEFT JOIN (
    SELECT TOP 1 szDoId, szDoIdOld 
    FROM MSYN_INV_HistoryDODn 
    ORDER BY dtmLastUpdated DESC
) Hist ON Hist.szDoIdOld = do.szDoId
LEFT JOIN BOS_SD_FDo doHist ON Hist.szDoId = doHist.szDoId
LEFT JOIN BOS_SD_FInvoice invoiceHist ON invoiceHist.szFInvoiceId = doHist.szFInvoiceId
LEFT JOIN BOS_SD_FInvoiceItem invItemHist ON invItemHist.szFInvoiceId = invoiceHist.szFInvoiceId 
AND invItemHist.szProductId = soi.szProductId
LEFT JOIN MSYN_INT_PurchaseCancelOrder cnclOrd ON si.szFSoId = cnclOrd.szDocId
LEFT JOIN BOS_GEN_Approved rejected ON so.gdApprovedId = rejected.gdApprovedId
WHERE 1=1 
AND (soi.szOrderItemTypeId = 'JUAL' OR poi.purchaseItemTypeId = 'BELI' OR poi.purchaseItemTypeId = 'BELI-BERIKAT')
AND (
    (si.szFPoId <> '' AND po.dtmPO >= '2024-10-01' AND po.dtmPO < '2025-01-01')
    OR (si.szFPoId = '' AND so.dtmOrder >= '2024-10-01' AND so.dtmOrder < '2025-01-01')
) 
AND delv.outTime  >= '2024-12-01' AND delv.outTime  < '2025-01-01'
AND custCat.szDescription IN ('GT', 'MTI')
AND ISNULL(cnclOrd.bAlreadyTransferred, 0) <> 1
AND ISNULL(rejected.bRejected, 0) <> 1
"""

# Connect to SQL Server using SQLAlchemy
engine = create_engine(sql_server_connection_str)

def fetch_data(sql_query):
    """Fetch data from SQL Server using the provided query."""
    df = pd.read_sql(sql_query, engine)
    return df

# Fetch data from SQL Server
df = fetch_data(sql_query)
print("Data fetched from SQL Server successfully.")

# Convert the 'OrderDate' column to string format to avoid pyarrow conversion issues
if 'date_created' in df.columns:
    df['date_created'] = df['date_created'].astype(str)

# Google BigQuery connection details
credentials_gbq = 'skintific-data-warehouse-ea77119e2e7a.json'
project_id = 'skintific-data-warehouse'
dataset_id = 'bosnet'
table_id = 'po_tracking_mtd_v1a'

# Set up credentials for BigQuery
credentials = service_account.Credentials.from_service_account_file(credentials_gbq)
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project_id

# Initialize BigQuery client
client = bigquery.Client(credentials=credentials)

# Step 1: Clear the target table before inserting new data
def clear_target_table():
    query = f"DELETE FROM `{project_id}.{dataset_id}.{table_id}` WHERE TRUE"
    client.query(query).result()  # Execute the query and wait for the result

# Clear the table
clear_target_table()
print(f"Table {table_id} cleared successfully.")

# Step 2: Upload new data to the cleared table
def upload_to_gbq(df_chunk):
    pandas_gbq.to_gbq(df_chunk, f"{dataset_id}.{table_id}", project_id=project_id, if_exists='replace')

upload_to_gbq(df)
print("Data uploaded to BigQuery successfully.")