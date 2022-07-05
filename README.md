#### S2S

##### Project gives an implementation of SQL to SQl ingestion scripts


#### To RUN
    python3 -m venv s2s-env
    pip install -r requirements.txt
    python -m sql_to_sql.sql_to_sql
    crontab -e
        */25 * * * * cd /path/to/project && s2s-env/bin/python3 -m sql_to_sql.sql_to_sql