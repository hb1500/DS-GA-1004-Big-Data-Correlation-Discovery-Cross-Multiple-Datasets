# DS-GA 1004 Project Original Proposal
## New proposal will be posted soon
Hetian Bai (hb1500), Jieyu Wang (jw4937), Zhiming Guo (zg758)


This is a brief proposal for DS-GA 1004 term project. In this project, we are aiming to develop an effective and scalable data exploration application that helps users to identify salient relationships between columns within and cross datasets. We will use tools such as Hadoop and Spark in data processing for their highly efficient and time-saving computational ability on large datasets. 

## Previous works and references

The main reference is "Data Polygamy: The Many-Many Relationships among Urban Spatio-Temporal Data Sets". (Chirigati, F., Doraiswamy, H., Damoulas, T., & Freire, J, 2015) Data Polygamy, as proposed by this paper, is "a scalable topology-based framework that allows users to query statistically significant relationships between spatio-temporal data sets". Researchers have also performed an experimental evaluation using over 300 spatial-temporal urban data sets which shows that this framework is scalable and effective at identifying interesting relationships. 

## Understanding the Problem, Problem Formulation and Goal

By the end of the project, we will build a reproductive framework which contains data pre-processing pipeline, correlation statistical analysis, and visualization of relationships between interested features. In addition, we will present this project framework and outputs in GitHub with clear content logic, user instruction, and relevant codes embedded with detailed descriptions for users. 

We decide to take the similar approach in previous work “Data Polygamy” as a proposed scheme of this project, which contains three steps: Dataset transformation, Feature identification, Relationship evaluation and Visualization. (detailed explanations in methodology)

For testing and presenting purpose, we will use our framework to find factors that correlated with NYC’s high school students’ Math test score and provide users with an online database that can be used to query a high school’s Math performance and its demographic, socioeconomic, and criminal statistics. 

## Datasets and Methodology

### Datasets: 

1. 2006 - 2011 NYS Math Test Results By Grade- School Level - By English Proficiency Status
2. 2010 - 2016 School Safety Report
3. 2006 - 2012 School Demographics and Accountability Snapshot
4. 2012 - 2017 Historical Monthly Grade Level Attendance By School

### Methodology: 

* Dataset Transformation: 

In this step, we will implement necessary data cleansing. Also, transform non-numeric data type into numeric which makes further computation possible. 

* Feature Identification:

First, we need to identify which columns of information are redundant in computing correlations. Then, we will do feature engineering. For example, slicing datasets by certain divisions like gender, class, school neighborhood, etc. Also, we could subset data by quantiles or outliners. After having all data columns, subsets ready, we calculate correlations with various measurements like Pearson, Kendall, Spearman, or self-defined measurements. 

* Relationship Evaluation and Visualization:

This attribution allows users to evaluate the strength and direction of relationships between any two attributions in the datasets. User will have options to set thresholds to filter on correlation results. 
A basic visualization function in the algorithms provide users with visualization to help them understand the relationships between attributes. For instance, we could make an elegant and well-labelled correlation matrix which represents the strength and direction of correlation by color. 

## Datasets Used:
(http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)
- [Taxi Data (Yellow)] 
(https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95)
- [Vehicle Collisions] 
(https://nyu.box.com/s/6epatrjp0bi8xvd17blzmoy301ikie9z)
- [Weather]
(https://nycopendata.socrata.com/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9)
- [311] 
(https://www.citibikenyc.com/system-data)
- [Citi Bike Trip Histories] 
(https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i)
- [Crime Data] 
(https://nyu.box.com/s/hx7v2mpsw7rkdps6b613tvoiq9a1r448)
- [Property Price] 
(http://www.nyc.gov/html/dcp/html/census/demo_tables_2010.shtml)
- [Census Data - demographics]
(http://www.nyc.gov/html/dcp/html/census/socio_tables.shtml)
- [Census Data - income information] 
(http://www.nyc.gov/html/dcp/html/bytes/districts_download_metadata.shtml)
- [Census Data (shape files)] 

## Evaluation: 

This project will be evaluated by the following four aspects: 

1. Time complexity
2. Scalability
3. Easiness to use 
4. Quality of visualization results 

## Reference: 

[1] Chirigati, F., Doraiswamy, H., Damoulas, T., & Freire, J. (2016, June). Data polygamy: the many-many relationships among urban spatio-temporal data sets. In Proceedings of the 2016 International Conference on Management of Data (pp. 1011-1025). ACM.

updated on: March 19th, 2018
