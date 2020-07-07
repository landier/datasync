import json

from snowflake import connector as snowflake


SNFLK_USER = "ITW_USER"
SNFLK_PASSWORD = "GhBAMXwHBtm-vJz7ewqR"
SNFLK_ACCOUNT = "db93014.eu-west-1"
SNFLK_ROLE = "ITW_ROLE"
SNFLK_WAREHOUSE = "WH_ITW"
SNFLK_DATABASE = "SPX_INTERVIEW"
SNFLK_SCHEMA = "PUBLIC"

RECORD_FIELDS = ['id',
                 'created_at',
                 'updated_at',
                 'archived_at',
                 'name',
                 'domain',
                 'phone',
                 'city',
                 'state',
                 'postal_code',
                 'country',
                 'locale',
                 'industry',
                 'company_type',
                 'founded',
                 'revenue_range',
                 'size',
                 'crm_id',
                 'crm_object_type',
                 'owner_crm_id',
                 'last_contacted_at',
                 'last_contacted_type',
                 'do_not_contact',
                 'custom_fields',
                 'tags',
                 'counts_people',
                 'owner',
                 'creator',
                 'last_contacted_by',
                 'last_contacted_person',
                 'company_stage',
                 'account_tier']

class DAL:
    def __init__(self):
        self.connection = snowflake.connect(
            user=SNFLK_USER,
            password=SNFLK_PASSWORD,
            account=SNFLK_ACCOUNT,
            warehouse=SNFLK_WAREHOUSE,
            database=SNFLK_DATABASE,
            schema=SNFLK_SCHEMA,
            role=SNFLK_ROLE,
        )

    def load_updated_records(self, records):
        # Not sure about the proper namespace to use a for temporary table
        self.connection.cursor().execute("CREATE TEMPORARY TABLE INTERVIEW.PUBLIC.UPDATED_ACCOUNTS (ID NUMBER, DATA VARIANT);")
        execute_query = (
            "INSERT INTO INTERVIEW.PUBLIC.UPDATED_ACCOUNTS (ID, DATA) "
            "SELECT Column1 AS ID, PARSE_JSON(Column2) AS DATA "
            "FROM VALUES "
        )
        fields = ""
        for r in records:
            fields += f"( {r['id']}, '{json.dumps(r)}'),"
        
        # Hacky query temination
        execute_query += fields[:-1] + ";"

        # print(query)

        self.connection.cursor().execute(execute_query)
        self.connection.commit()
        

    def push_to_db(self, records):
        self.load_updated_records(records)

        # Hereafter is an ugly query building... sorry for that.
        execute_query = "MERGE INTO INTERVIEW.PUBLIC.ACCOUNTS AS TARGET USING ( SELECT "
        
        select_fields = ""
        update_fields = ""
        values_fields = ""
        for f in RECORD_FIELDS:
            select_fields += f" DATA:{f} AS {f},"
            update_fields += f" TARGET.{f} = NEW_VALUES.{f},"
            values_fields += f" NEW_VALUES.{f},"
        
        execute_query += select_fields[:-1]

        execute_query += (
            " FROM INTERVIEW.PUBLIC.UPDATED_ACCOUNTS) AS NEW_VALUES  "
            "ON TARGET.ID = NEW_VALUES.ID "
            f"WHEN MATCHED THEN UPDATE SET {update_fields[:-1]} "
            f"WHEN NOT MATCHED THEN INSERT ({','.join(RECORD_FIELDS)}) "
            f"VALUES ({values_fields[:-1]}) ;"
        )
        # print(execute_query)
        result = self.connection.cursor().execute(execute_query).fetchone()
        self.connection.commit()
        print(f"Updated records in DB: {result[1]}")
