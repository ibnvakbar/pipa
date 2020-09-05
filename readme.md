#ODS NET - Nursyah's Engine Tools
Framework sederhana ini dibuat untuk mempermudah menulis dari ODS ke:
1. EDS
2. Staging Schema untuk Downstream
3. Downstream RDBMS menggunakan JDBC

## 1. [ods2eds] ODS to EDS
Parameter yang dibutuhkan untuk menjalankan modul ini adalah sebagai berikut:
- [m] Module Name: ods2eds
- [q] Script Filename, yang berada dalam SQL folder (cth. customer.sql)
- [t] Target Table Name (cth. CUSTOMER)
- [d] Business Date (format: yyyy-MM-dd; 0 for not using bussiness date)

Contoh penggunaan:
```
./run.sh -m ods2eds -q eds_customer.sql -t CUSTOMER -d 2020-01-01
```

## 2. [ods2staging] ODS to Staging
Parameter yang dibutuhkan untuk menjalankan modul ini adalah sebagai berikut:
- [m] Module Name: ods2staging
- [q] Script Filename, yang berada dalam SQL folder (cth. customer.sql)
- [t] Target Table Name (cth. STG_CUSTOMER)
- [d] Business Date (format: yyyy-MM-dd; 0 for not using bussiness date)

Contoh penggunaan:
```
./run.sh -m ods2staging -q stg_exus_customer.sql -t STG_CUSTOMER -d 0
```

## 3. [staging2jdbc] Staging to JDBC
Parameter yang dibutuhkan untuk menjalankan modul ini adalah sebagai berikut:
- [m] Module Name: staging2jdbc
- [q] Script Filename, yang berada dalam SQL folder (cth. customer.sql)
- [t] Target Table Name (cth. STG_CUSTOMER)
- [d] Business Date: 0

Contoh penggunaan:
```
./run.sh -m staging2jdbc -q exus_customer.sql -t PERSON -d 0
```
