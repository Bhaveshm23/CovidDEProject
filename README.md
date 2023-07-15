# CovidDEProject

The CovidDEProject aims to analyze COVID-19 data for various US states and counties. The project involves Extract, Transform, Load (ETL) processes and consists of 10 tables containing relevant COVID-19 datasets.

## Table details:

1. enigma_jhu
2. nytimes_data_in_usa_us_states
3. nytimes_data_in_usaus_county
4. rearc_covid_19_testing_data_states_daily
5. rearc_covid_19_testing_data_us_daily
6. rearc_covid_19_testing_data_us_total_latest
7. rearc_usa_hospital_beds
8. static_datasets_countrycode
9. static_datasets_countypopulation
10. static_datasets_state_abv

## Dataset Link:

The datasets used in this project can be accessed through the following link: ![Dataset Link](https://aws.amazon.com/covid-19-data-lake/)

## Project Flow:

1. **Data Collection and Storage:**
   - Data is collected from the provided link and stored in an S3 bucket.

2. **Data Extraction and Transformation:**
   - AWS Glue Crawler is used to extract the data from the S3 bucket and stores it in  S3.

3. **Data Query and Transformation:**
   - AWS Athena is utilized to query the data stored in S3. The data is transformed to create both fact and dimension tables.

   **Schema Details:**
   - The schema for the dimension and fact tables can be found in this ![Schema](https://github.com/Bhaveshm23/CovidDEProject/blob/main/Schema.png)

4. **Data Export:**
   - The transformed data is stored in S3 in CSV format.

5. **GLUE Job Execution:**
   - A GLUE job is developed to create the fact and dimension tables within the S3 bucket. The GLUE job is responsible for copying the data from S3 in CSV format to Redshift.

 **Process Flow:**
   - For a detailed visualization of the project's process flow, refer to this ![Process Flow](https://github.com/Bhaveshm23/CovidDEProject/blob/main/ProjectFlow.png)
