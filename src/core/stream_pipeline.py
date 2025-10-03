from pyspark.sql.functions import get_json_object, col,from_json,when,isnull

from .kafka_to_deltalake import *
from .schemas import *
from .consts import *





def stream_kafka_to_deltalake(spark, kafka_server):

    account_df=read_kafka_topic("postgres.public.account", spark, kafka_server)
    account_type_df=read_kafka_topic("postgres.public.account_type", spark, kafka_server)
    location_account_df=read_kafka_topic("postgres.public.location_account", spark, kafka_server)
    profile_image_df=read_kafka_topic("postgres.public.profile_image", spark, kafka_server)

    individual_df=read_kafka_topic("postgres.public.individual", spark, kafka_server)
    individual_course_or_training_df=read_kafka_topic("postgres.public.profile_courses", spark, kafka_server)
    individual_education_df = read_kafka_topic("postgres.public.education_profile", spark, kafka_server)
    individual_experience_df = read_kafka_topic("postgres.public.profile_experience", spark, kafka_server)
    individual_language_df = read_kafka_topic("postgres.public.profile_languages", spark, kafka_server)
    individual_skill_df = read_kafka_topic("postgres.public.profile_skills", spark, kafka_server)
    organization_df = read_kafka_topic("postgres.public.organization", spark, kafka_server)
    organization_section_df = read_kafka_topic("postgres.public.organization_section", spark, kafka_server)

    businesses_df = read_kafka_topic("postgres.public.primary_businesses", spark, kafka_server)
    companies_df = read_kafka_topic("postgres.public.companies", spark, kafka_server)
    locations_df = read_kafka_topic("postgres.public.locations", spark, kafka_server)
    courses_df = read_kafka_topic("postgres.public.courses", spark, kafka_server)
    degrees_df = read_kafka_topic("postgres.public.degrees", spark, kafka_server)
    employee_types_df = read_kafka_topic("postgres.public.employee_types", spark, kafka_server)
    help_types_df = read_kafka_topic("postgres.public.help_types", spark, kafka_server)
    countries_df = read_kafka_topic("postgres.public.countries", spark, kafka_server)
    honorifics_df = read_kafka_topic("postgres.public.honorifics", spark, kafka_server)
    policies_df = read_kafka_topic("postgres.public.policies", spark, kafka_server)
    positions_df = read_kafka_topic("postgres.public.positions", spark, kafka_server)
    professions_df = read_kafka_topic("postgres.public.professions", spark, kafka_server)
    specializations_df = read_kafka_topic("postgres.public.specializations", spark, kafka_server)
    tags_df = read_kafka_topic("postgres.public.tags", spark, kafka_server)
    user_tag_df = read_kafka_topic("postgres.public.user_tag", spark, kafka_server)
    universities_df = read_kafka_topic("postgres.public.universities", spark, kafka_server)
    states_df = read_kafka_topic("postgres.public.states", spark, kafka_server)
    languages_df = read_kafka_topic("postgres.public.languages", spark, kafka_server)




    ## account ##

    account_df_parsed = account_df.select(

        from_json(col("value").cast("string"), account_schema).alias("value"),

    )

    extracted_account_df = account_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.email")).otherwise(col("value.payload.after.email")).alias("email"),
        when(col("value.payload.op") == 'd', col("value.payload.before.password")).otherwise(col("value.payload.after.password")).alias("password"),
        when(col("value.payload.op") == 'd', col("value.payload.before.backupEmail")).otherwise(col("value.payload.after.backupEmail")).alias("backupEmail"),
        when(col("value.payload.op") == 'd', col("value.payload.before.twoStepVerification")).otherwise(col("value.payload.after.twoStepVerification")).alias("twoStepVerification"),
        when(col("value.payload.op") == 'd', col("value.payload.before.status")).otherwise(col("value.payload.after.status")).alias("status"),
        when(col("value.payload.op") == 'd', col("value.payload.before.phoneNumber")).otherwise(col("value.payload.after.phoneNumber")).alias("phoneNumber"),
        when(col("value.payload.op") == 'd', col("value.payload.before.deletedAt")).otherwise(col("value.payload.after.deletedAt")).alias("deletedAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_account_df = extracted_account_df.filter(
        ~(
            isnull(col("id")) #&
            # isnull(col("email")) &
            # isnull(col("password")) &
            # isnull(col("backupEmail")) &
            # isnull(col("twoStepVerification")) &
            # isnull(col("status")) &
            # isnull(col("phoneNumber")) &
            # isnull(col("deletedAt"))
        )
    )




    ## account_type ##

    account_type_df_parsed = account_type_df.select(

        from_json(col("value").cast("string"), account_type_schema).alias("value"),

    )

    extracted_account_type_df = account_type_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.accountId")).otherwise(col("value.payload.after.accountId")).alias("accountId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.type")).otherwise(col("value.payload.after.type")).alias("type"),
        when(col("value.payload.op") == 'd', col("value.payload.before.deletedAt")).otherwise(col("value.payload.after.deletedAt")).alias("deletedAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_account_type_df = extracted_account_type_df.filter(
        ~(
            isnull(col("accountId")) #&
            # isnull(col("type")) &
            # isnull(col("deletedAt"))
        )
    )




    ## location_account ##

    location_account_df_parsed = location_account_df.select(

        from_json(col("value").cast("string"), location_account_schema).alias("value"),

    )

    extracted_location_account_df = location_account_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.accountId")).otherwise(col("value.payload.after.accountId")).alias("accountId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.stateId")).otherwise(col("value.payload.after.stateId")).alias("stateId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.countryId")).otherwise(col("value.payload.after.countryId")).alias("countryId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.deletedAt")).otherwise(col("value.payload.after.deletedAt")).alias("deletedAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )


    filtered_location_account_df = extracted_location_account_df.filter(
        ~(
            isnull(col("accountId")) #&
            # isnull(col("stateId")) &
            # isnull(col("countryId")) &
            # isnull(col("deletedAt"))
        )
    )




    ## profile_image ##

    profile_image_df_parsed = profile_image_df.select(

        from_json(col("value").cast("string"), profile_image_schema).alias("value"),

    )

    extracted_profile_image_df = profile_image_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.accountId")).otherwise(col("value.payload.after.accountId")).alias("accountId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.deletedAt")).otherwise(col("value.payload.after.deletedAt")).alias("deletedAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_profile_image_df = extracted_profile_image_df.filter(
        ~(
            isnull(col("accountId"))
        )
    )



    ## individual ##

    individual_df_parsed = individual_df.select(

        from_json(col("value").cast("string"), individual_schema).alias("value"),

    )

    extracted_individual_df = individual_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.accountId")).otherwise(col("value.payload.after.accountId")).alias("accountId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.firstName")).otherwise(col("value.payload.after.firstName")).alias("firstName"),
        when(col("value.payload.op") == 'd', col("value.payload.before.lastName")).otherwise(col("value.payload.after.lastName")).alias("lastName"),
        when(col("value.payload.op") == 'd', col("value.payload.before.DOB")).otherwise(col("value.payload.after.DOB")).alias("DOB"),
        when(col("value.payload.op") == 'd', col("value.payload.before.gender")).otherwise(col("value.payload.after.gender")).alias("gender"),
        when(col("value.payload.op") == 'd', col("value.payload.before.honorificId")).otherwise(col("value.payload.after.honorificId")).alias("honorificId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.primaryProfessionId")).otherwise(col("value.payload.after.primaryProfessionId")).alias("primaryProfessionId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.deletedAt")).otherwise(col("value.payload.after.deletedAt")).alias("deletedAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )


    filtered_individual_df = extracted_individual_df.filter(
        ~(
            isnull(col("accountId"))
        )
    )


    ## individual_course_or_training ##

    individual_course_or_training_df_parsed = individual_course_or_training_df.select(

        from_json(col("value").cast("string"), individual_course_or_training_schema).alias("value"),

    )

    extracted_individual_course_or_training_df = individual_course_or_training_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.account_id")).otherwise(col("value.payload.after.account_id")).alias("accountId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.course_id")).otherwise(col("value.payload.after.course_id")).alias("courseId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.start_date")).otherwise(col("value.payload.after.start_date")).alias("startDate"),
        when(col("value.payload.op") == 'd', col("value.payload.before.end_date")).otherwise(col("value.payload.after.end_date")).alias("endDate"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("createdAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )



    filtered_individual_course_or_training_df = extracted_individual_course_or_training_df.filter(
        ~(
            isnull(col("id"))
        )
    )


    ## individual_education ##

    individual_education_df_parsed = individual_education_df.select(

        from_json(col("value").cast("string"), individual_education_schema).alias("value"),

    )

    extracted_individual_education_df = individual_education_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.account_id")).otherwise(col("value.payload.after.account_id")).alias("accountId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.start_date")).otherwise(col("value.payload.after.start_date")).alias("startDate"),
        when(col("value.payload.op") == 'd', col("value.payload.before.end_date")).otherwise(col("value.payload.after.end_date")).alias("endDate"),
        when(col("value.payload.op") == 'd', col("value.payload.before.degree_id")).otherwise(col("value.payload.after.degree_id")).alias("degreeId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.miner_id")).otherwise(col("value.payload.after.miner_id")).alias("minerId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.major_id")).otherwise(col("value.payload.after.major_id")).alias("majorId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.university_id")).otherwise(col("value.payload.after.university_id")).alias("universityId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("createdAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )



    filtered_individual_education_df = extracted_individual_education_df.filter(
        ~(
            isnull(col("id"))
        )
    )



    ## individual_experience ##

    individual_experience_df_parsed = individual_experience_df.select(

        from_json(col("value").cast("string"), individual_experience_schema).alias("value"),

    )

    extracted_individual_experience_df = individual_experience_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.account_id")).otherwise(col("value.payload.after.account_id")).alias("accountId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.start_date")).otherwise(col("value.payload.after.start_date")).alias("startDate"),
        when(col("value.payload.op") == 'd', col("value.payload.before.end_date")).otherwise(col("value.payload.after.end_date")).alias("endDate"),
        when(col("value.payload.op") == 'd', col("value.payload.before.company_id")).otherwise(col("value.payload.after.company_id")).alias("companyId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.employment_type_id")).otherwise(col("value.payload.after.employment_type_id")).alias("employmentTypeId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.position_id")).otherwise(col("value.payload.after.position_id")).alias("positionId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("createdAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )



    filtered_individual_experience_df = extracted_individual_experience_df.filter(
        ~(
            isnull(col("id"))
        )
    )





    ## individual_language ##

    individual_language_df_parsed = individual_language_df.select(

        from_json(col("value").cast("string"), individual_language_schema).alias("value"),

    )

    extracted_individual_language_df = individual_language_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.account_id")).otherwise(col("value.payload.after.account_id")).alias("accountId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.language_level")).otherwise(col("value.payload.after.language_level")).alias("level"),
        when(col("value.payload.op") == 'd', col("value.payload.before.language_id")).otherwise(col("value.payload.after.language_id")).alias("languageId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("createdAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )


    filtered_individual_language_df = extracted_individual_language_df.filter(
        ~(
            isnull(col("id"))
        )
    )




    ## individual_skill ##

    individual_skill_df_parsed = individual_skill_df.select(

        from_json(col("value").cast("string"), individual_skill_schema).alias("value"),

    )

    extracted_individual_skill_df = individual_skill_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.account_id")).otherwise(col("value.payload.after.account_id")).alias("accountId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.skill_name")).otherwise(col("value.payload.after.skill_name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("createdAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )


    filtered_individual_skill_df = extracted_individual_skill_df.filter(
        ~(
            isnull(col("id"))
        )
    )





    ## organization ##

    organization_df_parsed = organization_df.select(

        from_json(col("value").cast("string"), organization_schema).alias("value"),

    )

    extracted_organization_df = organization_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.accountId")).otherwise(col("value.payload.after.accountId")).alias("accountId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.createAccountBy")).otherwise(col("value.payload.after.createAccountBy")).alias("createAccountBy"),
        when(col("value.payload.op") == 'd', col("value.payload.before.relationToOrganization")).otherwise(col("value.payload.after.relationToOrganization")).alias("relationToOrganization"),
        when(col("value.payload.op") == 'd', col("value.payload.before.website")).otherwise(col("value.payload.after.website")).alias("website"),
        when(col("value.payload.op") == 'd', col("value.payload.before.primaryBusinessId")).otherwise(col("value.payload.after.primaryBusinessId")).alias("primaryBusinessId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.deletedAt")).otherwise(col("value.payload.after.deletedAt")).alias("deletedAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )


    filtered_organization_df = extracted_organization_df.filter(
        ~(
            isnull(col("accountId"))
        )
    )




    ## organization_section ##

    organization_section_df_parsed = organization_section_df.select(

        from_json(col("value").cast("string"), organization_section_schema).alias("value"),

    )

    extracted_organization_section_df = organization_section_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.account_id")).otherwise(col("value.payload.after.account_id")).alias("accountId"),
        when(col("value.payload.op") == 'd', col("value.payload.before.Title")).otherwise(col("value.payload.after.Title")).alias("title"),
        when(col("value.payload.op") == 'd', col("value.payload.before.description")).otherwise(col("value.payload.after.description")).alias("description"),
        when(col("value.payload.op") == 'd', col("value.payload.before.index")).otherwise(col("value.payload.after.index")).alias("index"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("createdAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_organization_section_df = extracted_organization_section_df.filter(
        ~(
            isnull(col("id"))
        )
    )



    ## businesses ##

    businesses_df_parsed = businesses_df.select(

        from_json(col("value").cast("string"), businesses_schema).alias("value"),

    )

    extracted_businesses_df = businesses_df_parsed.select(
        
        col("value.payload.op").alias("operationType"),
        
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.status")).otherwise(col("value.payload.after.status")).alias("status"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.createdAt")).otherwise(col("value.payload.after.createdAt")).alias("createdAt"),
        when(col("value.payload.op") == 'd', col("value.payload.before.updated_at")).otherwise(col("value.payload.after.updated_at")).alias("updated_at"),
        

        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_businesses_df = extracted_businesses_df.filter(
        ~(
            isnull(col("id"))
        )
    )




    ## companies ##

    companies_df_parsed = companies_df.select(

        from_json(col("value").cast("string"), companies_schema).alias("value"),

    )

    extracted_companies_df = companies_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.status")).otherwise(col("value.payload.after.status")).alias("status"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.createdAt")).otherwise(col("value.payload.after.createdAt")).alias("createdAt"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )


    filtered_companies_df = extracted_companies_df.filter(
        ~(
            isnull(col("id"))
        )
    )




    ## locations ##

    locations_df_parsed = locations_df.select(

        from_json(col("value").cast("string"), locations_schema).alias("value"),

    )

    extracted_locations_df = locations_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.state_id")).otherwise(col("value.payload.after.state_id")).alias("state_id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.description")).otherwise(col("value.payload.after.description")).alias("description"),
        when(col("value.payload.op") == 'd', col("value.payload.before.company_id")).otherwise(col("value.payload.after.company_id")).alias("company_id"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_locations_df = extracted_locations_df.filter(
        ~(
            isnull(col("id"))
        )
    )






    ## courses ##

    courses_df_parsed = courses_df.select(

        from_json(col("value").cast("string"), courses_schema).alias("value"),

    )

    extracted_courses_df = courses_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.status")).otherwise(col("value.payload.after.status")).alias("status"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )


    filtered_courses_df = extracted_courses_df.filter(
        ~(
            isnull(col("id"))
        )
    )




    ## degrees ##

    degrees_df_parsed = degrees_df.select(

        from_json(col("value").cast("string"), degrees_schema).alias("value"),

    )

    extracted_degrees_df = degrees_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.status")).otherwise(col("value.payload.after.status")).alias("status"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_degrees_df = extracted_degrees_df.filter(
        ~(
            isnull(col("id"))
        )
    )


    ## employee_types ##

    employee_types_df_parsed = employee_types_df.select(

        from_json(col("value").cast("string"), employee_types_schema).alias("value"),

    )

    extracted_employee_types_df = employee_types_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.status")).otherwise(col("value.payload.after.status")).alias("status"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("created_at"),
        when(col("value.payload.op") == 'd', col("value.payload.before.updated_at")).otherwise(col("value.payload.after.updated_at")).alias("updated_at"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )


    filtered_employee_types_df = extracted_employee_types_df.filter(
        ~(
            isnull(col("id"))
        )
    )


    ## help_types ##

    help_types_df_parsed = help_types_df.select(

        from_json(col("value").cast("string"), help_types_schema).alias("value"),

    )

    extracted_help_types_df = help_types_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.title")).otherwise(col("value.payload.after.title")).alias("title"),
        when(col("value.payload.op") == 'd', col("value.payload.before.description")).otherwise(col("value.payload.after.description")).alias("description"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("created_at"),
        when(col("value.payload.op") == 'd', col("value.payload.before.updated_at")).otherwise(col("value.payload.after.updated_at")).alias("updated_at"),
        when(col("value.payload.op") == 'd', col("value.payload.before.page")).otherwise(col("value.payload.after.page")).alias("page"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_help_types_df = extracted_help_types_df.filter(
        ~(
            isnull(col("id"))
        )
    )




    ## countries ##

    countries_df_parsed = countries_df.select(

        from_json(col("value").cast("string"), countries_schema).alias("value"),

    )

    extracted_countries_df = countries_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.code")).otherwise(col("value.payload.after.code")).alias("code"),

        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_countries_df = extracted_countries_df.filter(
        ~(
            isnull(col("id"))
        )
    )


    ## honorifics ##

    honorifics_df_parsed = honorifics_df.select(

        from_json(col("value").cast("string"), honorifics_schema).alias("value"),

    )

    extracted_honorifics_df = honorifics_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.status")).otherwise(col("value.payload.after.status")).alias("status"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("created_at"),
        when(col("value.payload.op") == 'd', col("value.payload.before.updated_at")).otherwise(col("value.payload.after.updated_at")).alias("updated_at"),

        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_honorifics_df = extracted_honorifics_df.filter(
        ~(
            isnull(col("id"))
        )
    )




    ## positions ##

    positions_df_parsed = positions_df.select(

        from_json(col("value").cast("string"), positions_schema).alias("value"),

    )

    extracted_positions_df = positions_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.status")).otherwise(col("value.payload.after.status")).alias("status"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("created_at"),
        when(col("value.payload.op") == 'd', col("value.payload.before.updated_at")).otherwise(col("value.payload.after.updated_at")).alias("updated_at"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_positions_df = extracted_positions_df.filter(
        ~(
            isnull(col("id"))
        )
    )




    ## professions ##

    professions_df_parsed = professions_df.select(

        from_json(col("value").cast("string"), professions_schema).alias("value"),

    )

    extracted_professions_df = professions_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.status")).otherwise(col("value.payload.after.status")).alias("status"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("created_at"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )


    filtered_professions_df = extracted_professions_df.filter(
        ~(
            isnull(col("id"))
        )
    )




    ## tags ##

    tags_df_parsed = tags_df.select(

        from_json(col("value").cast("string"), tags_schema).alias("value"),

    )

    extracted_tags_df = tags_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.count")).otherwise(col("value.payload.after.count")).alias("count"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("created_at"),
        when(col("value.payload.op") == 'd', col("value.payload.before.updated_at")).otherwise(col("value.payload.after.updated_at")).alias("updated_at"),
        when(col("value.payload.op") == 'd', col("value.payload.before.deleted_at")).otherwise(col("value.payload.after.deleted_at")).alias("deleted_at"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_tags_df = extracted_tags_df.filter(
        ~(
            isnull(col("id"))
        )
    )



    ## user_tag ##

    user_tag_df_parsed = user_tag_df.select(

        from_json(col("value").cast("string"), user_tag_schema).alias("value"),

    )

    extracted_user_tag_df = user_tag_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.account_id")).otherwise(col("value.payload.after.account_id")).alias("account_id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.tag_id")).otherwise(col("value.payload.after.tag_id")).alias("tag_id"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )


    filtered_user_tag_df = extracted_user_tag_df.filter(
        ~(
            isnull(col("account_id"))&
            isnull(col("tag_id"))
        )
    )


    ## universities ##

    universities_df_parsed = universities_df.select(

        from_json(col("value").cast("string"), universities_schema).alias("value"),

    )

    extracted_universities_df = universities_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.status")).otherwise(col("value.payload.after.status")).alias("status"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("created_at"),
        when(col("value.payload.op") == 'd', col("value.payload.before.updated_at")).otherwise(col("value.payload.after.updated_at")).alias("updated_at"),
        
        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_universities_df = extracted_universities_df.filter(
        ~(
            isnull(col("id"))
        )
    )


    ## states ##

    states_df_parsed = states_df.select(

        from_json(col("value").cast("string"), states_schema).alias("value"),

    )

    extracted_states_df = states_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.country_id")).otherwise(col("value.payload.after.country_id")).alias("country_id"),

        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_states_df = extracted_states_df.filter(
        ~(
            isnull(col("id"))
        )
    )


    ## languages ##

    languages_df_parsed = languages_df.select(

        from_json(col("value").cast("string"), languages_schema).alias("value"),

    )

    extracted_languages_df = languages_df_parsed.select(
        when(col("value.payload.op") == 'd', col("value.payload.before.id")).otherwise(col("value.payload.after.id")).alias("id"),
        when(col("value.payload.op") == 'd', col("value.payload.before.name")).otherwise(col("value.payload.after.name")).alias("name"),
        when(col("value.payload.op") == 'd', col("value.payload.before.status")).otherwise(col("value.payload.after.status")).alias("status"),
        when(col("value.payload.op") == 'd', col("value.payload.before.created_at")).otherwise(col("value.payload.after.created_at")).alias("created_at"),
        when(col("value.payload.op") == 'd', col("value.payload.before.updated_at")).otherwise(col("value.payload.after.updated_at")).alias("updated_at"),

        col("value.payload.op").alias("operationType"),
        
        col("value.payload.source.table").alias("table_name"),
        col("value.payload.source.db").alias("database_name"),
        col("value.payload.source.schema").alias("schema_name"),
    )

    filtered_languages_df = extracted_languages_df.filter(
        ~(
            isnull(col("id"))
        )
    )


#
    write_to_delta(filtered_account_df, merge_to_delta, account_path, "account", "id")
    write_to_delta(filtered_account_type_df, merge_to_delta, account_type_path, "account_type", "accountId")
   
   
   
    write_to_delta(filtered_location_account_df, merge_to_delta, location_account_path, "location_account", "accountId")
    write_to_delta(filtered_profile_image_df, merge_to_delta, profile_image_path, "profile_image", "accountId")



#
    write_to_delta(filtered_individual_df, merge_to_delta, individual_path, "individual", "accountId")
    write_to_delta(filtered_individual_course_or_training_df, merge_to_delta, individual_course_or_training_path, "individual_course_or_training", "id")
    write_to_delta(filtered_individual_education_df, merge_to_delta, individual_education_path, "individual_education", "id")
    write_to_delta(filtered_individual_experience_df, merge_to_delta, individual_experience_path, "individual_experience", "id")
    

    
    write_to_delta(filtered_individual_language_df, merge_to_delta, individual_language_path, "individual_language", "id")
    write_to_delta(filtered_individual_skill_df, merge_to_delta, individual_skill_path, "individual_skill", "id")

    write_to_delta(filtered_organization_df, merge_to_delta, organization_path, "organization", "accountId")
    write_to_delta(filtered_organization_section_df, merge_to_delta, organization_section_path, "organization_section", "id").awaitTermination()



    write_to_delta(filtered_businesses_df, merge_to_delta, businesses_path, "businesses", "id")
    write_to_delta(filtered_companies_df, merge_to_delta, companies_path, "companies", "id")
    write_to_delta(filtered_locations_df, merge_to_delta, locations_path, "locations", "id")
    write_to_delta(filtered_courses_df, merge_to_delta, courses_path, "courses", "id")
    write_to_delta(filtered_degrees_df, merge_to_delta, degrees_path, "degrees", "id")

    write_to_delta(filtered_employee_types_df, merge_to_delta, employee_types_path, "employee_types", "id")
    write_to_delta(filtered_help_types_df, merge_to_delta, help_types_path, "help_types", "id")
    write_to_delta(filtered_countries_df, merge_to_delta, countries_path, "countries", "id")
    write_to_delta(filtered_honorifics_df, merge_to_delta, honorifics_path, "honorifics", "id")
    write_to_delta(filtered_positions_df, merge_to_delta, positions_path, "positions", "id")
    write_to_delta(filtered_professions_df, merge_to_delta, professions_path, "professions", "id")
    write_to_delta(filtered_tags_df, merge_to_delta, tags_path, "tags", "id")
    write_to_delta(filtered_user_tag_df, merge_to_delta, user_tag_path, "user_tag", "id").awaitTermination()
    write_to_delta(filtered_universities_df, merge_to_delta, universities_path, "universities", "id").awaitTermination()
    write_to_delta(filtered_states_df, merge_to_delta, states_path, "states", "id")
    write_to_delta(filtered_languages_df, merge_to_delta, languages_path, "languages", "id").awaitTermination()




