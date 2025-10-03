from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, TimestampType


## account ##

account_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("email", StringType(), True),
            StructField("password", StringType(), True),
            StructField("backupEmail", StringType(), True),
            StructField("twoStepVerification", BooleanType(), True),
            StructField("status", StringType(), True),
            StructField("phoneNumber", StringType(), True),
            StructField("deletedAt", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), False),
            StructField("email", StringType(), False),
            StructField("password", StringType(), False),
            StructField("backupEmail", StringType(), True),
            StructField("twoStepVerification", BooleanType(), False),
            StructField("status", StringType(), False),
            StructField("phoneNumber", StringType(), True),
            StructField("deletedAt", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])



## account_type ##

account_type_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("accountId", StringType(), True),
            StructField("type", StringType(), True),
            StructField("deletedAt", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("accountId", StringType(), False),
            StructField("type", StringType(), False),
            StructField("deletedAt", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])



## location_account ##

location_account_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("accountId", StringType(), True),
            StructField("stateId", StringType(), True),
            StructField("countryId", StringType(), True),
            StructField("deletedAt", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("accountId", StringType(), True),
            StructField("stateId", StringType(), True),
            StructField("countryId", StringType(), True),
            StructField("deletedAt", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])




## profile_image ##

profile_image_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("accountId", StringType(), True),
            StructField("name", StringType(), True),
            StructField("deletedAt", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("accountId", StringType(), True),
            StructField("name", StringType(), True),
            StructField("deletedAt", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])






## individual ##

individual_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("accountId", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("DOB", TimestampType(), True),
            StructField("gender", StringType(), True),
            StructField("honorificId", StringType(), True),
            StructField("primaryProfessionId", StringType(), True),
            StructField("deletedAt", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("accountId", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("DOB", TimestampType(), True),
            StructField("gender", StringType(), True),
            StructField("honorificId", StringType(), True),
            StructField("primaryProfessionId", StringType(), True),
            StructField("deletedAt", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])




## individual_course_or_training ##

individual_course_or_training_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("course_id", StringType(), True),
            StructField("start_date", TimestampType(), True),
            StructField("end_date", TimestampType(), True),
            StructField("created_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("course_id", StringType(), True),
            StructField("start_date", TimestampType(), True),
            StructField("end_date", TimestampType(), True),
            StructField("created_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])




## individual_education ##

individual_education_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("start_date", TimestampType(), True),
            StructField("end_date", TimestampType(), True),
            StructField("degree_id", StringType(), True),
            StructField("miner_id", StringType(), True),
            StructField("major_id", StringType(), True),
            StructField("university_id", StringType(), True),
            StructField("created_at", TimestampType(), True)

        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("start_date", TimestampType(), True),
            StructField("end_date", TimestampType(), True),
            StructField("degree_id", StringType(), True),
            StructField("miner_id", StringType(), True),
            StructField("major_id", StringType(), True),
            StructField("university_id", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])




## individual_experience ##

individual_experience_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("start_date", TimestampType(), True),
            StructField("end_date", TimestampType(), True),
            StructField("company_id", StringType(), True),
            StructField("employment_type_id", StringType(), True),
            StructField("position_id", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("start_date", TimestampType(), True),
            StructField("end_date", TimestampType(), True),
            StructField("company_id", StringType(), True),
            StructField("employment_type_id", StringType(), True),
            StructField("position_id", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])



## individual_language ##

individual_language_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("language_level", StringType(), True),
            StructField("language_id", StringType(), True),
            StructField("created_at", TimestampType(), True)

        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("language_level", StringType(), True),
            StructField("language_id", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])



    
## individual_skill ##

individual_skill_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("skill_name", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("skill_name", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])



    
## organization ##

organization_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("accountId", StringType(), True),
            StructField("name", StringType(), True),
            StructField("createAccountBy", StringType(), True),
            StructField("relationToOrganization", StringType(), True),
            StructField("website", StringType(), True),
            StructField("primaryBusinessId", StringType(), True),
            StructField("deletedAt", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("accountId", StringType(), True),
            StructField("name", StringType(), True),
            StructField("createAccountBy", StringType(), True),
            StructField("relationToOrganization", StringType(), True),
            StructField("website", StringType(), True),
            StructField("primaryBusinessId", StringType(), True),
            StructField("deletedAt", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])




## organization_section ##

organization_section_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("Title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("index", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("Title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("index", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])





## businesses ##

businesses_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("name", StringType(), True),
            StructField("createdAt", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("name", StringType(), True),
            StructField("createdAt", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])







## companies ##

companies_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("name", StringType(), True),
            StructField("createdAt", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("name", StringType(), True),
            StructField("createdAt", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])



## locations ##

locations_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("state_id", StringType(), True),
            StructField("description", StringType(), True),
            StructField("company_id", StringType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("state_id", StringType(), True),
            StructField("description", StringType(), True),
            StructField("company_id", StringType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])





## courses ##

courses_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("name", StringType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("name", StringType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])




## degrees ##

degrees_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("name", StringType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("name", StringType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])




## employee_types ##

employee_types_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("name", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("name", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])





## help_types ##

help_types_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("page", StringType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("page", StringType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])







## countries ##

countries_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("code", StringType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("code", StringType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])



## honorifics ##

honorifics_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])




## policies ##

policies_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("type", StringType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("type", StringType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])



## positions ##

positions_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])




## professions ##

professions_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])



## specializations ##

specializations_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])


## tags ##

tags_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("count", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("deleted_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("count", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("deleted_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])




## user_tag ##

user_tag_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("account_id", StringType(), True),
            StructField("tag_id", StringType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("account_id", StringType(), True),
            StructField("tag_id", StringType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])



## universities ##

universities_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])




## states ##

states_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("country_id", StringType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("country_id", StringType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])



## languages ##

languages_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ]), True),
        
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True)
    ]))
])