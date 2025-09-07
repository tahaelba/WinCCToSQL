Files in this bundle:

1) decode_tagcompressed_dc.py
   - Decode digital (_DC) TagCompressed rows into timestamp,value CSV.
   - SQL auth or --trusted supported.

2) export_dc_tags_bulk.py
   - Export ALL *_DC tags to CSV files (one per tag).

3) export_sqlserver_to_csv.py
   - Generic table exporter for any tables/views.

4) requirements.txt
   - Install with:   pip install -r requirements.txt

Notes:
- From WSL/Linux/macOS you need unixODBC + msodbcsql18 installed.
- Use SERVER=<WindowsHostIP>,1433 with SQL authentication from WSL.
