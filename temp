(venv) PS C:\Users\W0024618\Desktop\Trend-Analysis\backend> python app.py
 * Serving Flask app 'app'
 * Debug mode: on
INFO:werkzeug:WARNING: This is a development server. Do not use it in a production deployment. Use a production WSGI server instead.
 * Running on all addresses (0.0.0.0)
 * Running on http://127.0.0.1:8002
 * Running on http://10.199.47.133:8002
INFO:werkzeug:Press CTRL+C to quit
INFO:werkzeug: * Restarting with stat
WARNING:werkzeug: * Debugger is active!
INFO:werkzeug: * Debugger PIN: 123-223-392
INFO:root:Fetching swipes for region apac on 2025-12-10
ERROR:root:Failed to connect to master DB for server SRVWUPNQ0986V
Traceback (most recent call last):
  File "C:\Users\W0024618\Desktop\Trend-Analysis\backend\duration_report.py", line 344, in _connect_master
    return pyodbc.connect(conn_str, autocommit=True)
           ~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^
pyodbc.InterfaceError: ('IM002', '[IM002] [Microsoft][ODBC Driver Manager] Data source name not found and no default driver specified (0) (SQLDriverConnect)')
WARNING:root:Unable to validate DB existence (no master connection). Proceeding with candidate list: ['ACVSUJournal_00010030', 'ACVSUJournal_00010029', 'ACVSUJournal_00010028', 'ACVSUJournal_00010027', 'ACVSUJournal_00010026', 'ACVSUJournal_00010025']
INFO:root:Built SQL for region apac, date 2025-12-10
ERROR:root:Failed to run query for region apac
Traceback (most recent call last):
  File "C:\Users\W0024618\Desktop\Trend-Analysis\backend\duration_report.py", line 455, in fetch_swipes_for_region
    conn = get_connection(region_key)
  File "C:\Users\W0024618\Desktop\Trend-Analysis\backend\duration_report.py", line 432, in get_connection 
    return pyodbc.connect(conn_str, autocommit=True)
           ~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^
pyodbc.InterfaceError: ('IM002', '[IM002] [Microsoft][ODBC Driver Manager] Data source name not found and no default driver specified (0) (SQLDriverConnect)')
INFO:root:Wrote duration CSV for apac to C:\Users\W0024618\Desktop\Trend-Analysis\backend\outputs\apac_duration_20251210.csv (rows=0)
INFO:root:Wrote swipes CSV for apac to C:\Users\W0024618\Desktop\Trend-Analysis\backend\outputs\apac_swipes_20251210.csv (rows=0)
WARNING:root:run_trend_for_date: no features computed
INFO:werkzeug:127.0.0.1 - - [11/Dec/2025 15:20:15] "GET /run?start=2025-12-10&end=2025-12-10&full=true&region=apac HTTP/1.1" 200 -
