## S2S / S2S3 ![visitor badge](https://visitor-badge.glitch.me/badge?page_id=hanchau.s2s3&left_text=VisitorsSoFar)



### Project gives an implementation of SQL to SQl & S3 ingestion

#### To RUN
    python3 -m venv s2s-env
    pip install -r requirements.txt
    python -m sql_to_sql.s2s

    # add the s3 acces id and keys in the environment.sh
    chmod +x environment.sh
    source ./environment.sh
    python -m sql_to_sql.s2s3

#### Crontab
    crontab -e
        */25 * * * * cd /path/to/project && s2s-env/bin/python3 -m sql_to_sql.s2s
        */50 * * * * cd /path/to/project && s2s-env/bin/python3 -m sql_to_s3.s2s3
