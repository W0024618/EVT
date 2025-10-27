PS D:\DASHBOARD\global-page\backend\attendance-analytics> .\.venv\Scripts\Activate.ps1
(.venv) PS D:\DASHBOARD\global-page\backend\attendance-analytics> python -m uvicorn app:app --host 0.0.0.0 --port 8001
INFO:     Started server process [9956]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8001 (Press CTRL+C to quit)
2025-10-27 11:31:40,037 INFO attendance_app: [5825055b] denver-attendance request received: year=None month=None from_date=2025-08-01 to_date=2025-10-31
2025-10-27 11:31:40,039 INFO attendance_app: [5825055b] generating denver report for range 2025-08-01 -> 2025-10-31
2025-10-27 11:33:01,244 ERROR attendance_app: [5825055b] Report generation failed
Traceback (most recent call last):
  File "D:\DASHBOARD\global-page\backend\attendance-analytics\app.py", line 5288, in api_denver_attendance
    path = denver_mod.generate_monthly_denver_report(start_date=start_dt, end_date=end_dt, outdir=str(OUTPUT_DIR))
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\DASHBOARD\global-page\backend\attendance-analytics\denverAttendance.py", line 169, in generate_monthly_denver_report
    return _write_excel(df, ordered_days, months, outdir, start_date, end_date)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\DASHBOARD\global-page\backend\attendance-analytics\denverAttendance.py", line 176, in _write_excel
    import xlsxwriter
ModuleNotFoundError: No module named 'xlsxwriter'
INFO:     10.199.22.57:60997 - "GET /reports/denver-attendance?from_date=2025-08-01&to_date=2025-10-31 HTTP/1.1" 500 Internal Server Error
INFO:     10.199.46.101:51745 - "GET /ccure/stream HTTP/1.1" 200 OK
INFO:     10.199.22.57:61047 - "GET /ccure/verify?raw=true HTTP/1.1" 200 OK
