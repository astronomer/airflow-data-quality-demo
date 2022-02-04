CREATE OR REPLACE STAGE {{ params.stage_name }} url=s3://{{ var.json.aws_configs.s3_bucket }}
credentials=(aws_key_id='{{ conn.aws_default.login }}' aws_secret_key='{{ conn.aws_default.password }}')
file_format=(type = 'CSV', skip_header = 1, time_format = 'YYYY-MM-DD HH24:MI:SS');
