#### S2S / S2S3

##### Project gives an implementation of SQL to SQl & S3 ingestion

#### To RUN
    python3 -m venv s2s-env
    pip install -r requirements.txt
    python -m sql_to_sql.s2s

    # add the s3 acces id and keys in the environment.sh
    chmod +x environment.sh
    source ./environment.sh
    python -m sql_to_sql.s2s3

#### CRONTAB
    crontab -e
        */25 * * * * cd /path/to/project && s2s-env/bin/python3 -m sql_to_sql.s2s
        */25 * * * * cd /path/to/project && s2s-env/bin/python3 -m sql_to_s3.s2s3
