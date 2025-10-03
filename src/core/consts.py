import os

base_path = os.environ['S3_USER_PATH']


account_path = "account/"
account_type_path = "account_type/"
location_account_path = "location_account/"
profile_image_path = "profile_image/"

individual_path = "individual/"
individual_course_or_training_path= "individual_course_or_training/"
individual_education_path= "individual_education/"
individual_experience_path= "individual_experience/"
individual_language_path= "individual_language/"
individual_skill_path= "individual_skill/"
organization_path= "organization/"
organization_section_path= "organization_section/"

businesses_path= "businesses/"
companies_path= "companies/"
locations_path= "locations/" 
courses_path= "courses/"  
degrees_path= "degrees/" 
employee_types_path= "employee_types/"
help_types_path= "help_types/" 
countries_path= "countries/"  
honorifics_path= "honorifics/" 
policies_path= "policies/"  
positions_path= "positions/" 
professions_path= "professions/" 
specializations_path= "specializations/" 
tags_path= "tags/" 
user_tag_path= "user_tag/" 
universities_path= "universities/" 
states_path= "states/" 
languages_path= "languages/"





paths = [
    "account/", "account_type/", "location_account/", "profile_image/", "individual/",
    "individual_course_or_training/", "individual_education/", "individual_experience/",
    "individual_language/", "individual_skill/", "organization/", "organization_section/",
    "businesses/", "companies/", "locations/", "courses/", "degrees/",
    "employee_types/", "help_types/", "countries/", "honorifics/", "policies/",
    "positions/", "professions/", "tags/", "user_tag/",
    "universities/", "states/", "languages/"
]


table_names = [
    "account", "account_type", "location_account", "profile_image", "individual",
    "individual_course_or_training", "individual_education", "individual_experience",
    "individual_language", "individual_skill", "organization", "organization_section",
    "businesses", "companies", "locations", "courses", "degrees",
    "employee_types", "help_types", "countries", "honorifics", "policies",
    "positions", "professions","tags", "user_tag",
    "universities", "states", "languages"
]