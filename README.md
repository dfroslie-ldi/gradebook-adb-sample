# gradebook-adb-sample
Repo with sample code for a simple gradebook medallion architecture.

## How to use

### Set up storage
- Create an Azure Storage account and blob store.  
- Create a folder called TrainingFiles.  Upload StudentInfo.csv and Items.csv to this folder.
- Create a sub-folder called Grades.  Upload the Grades files to this folder.

### Set up Azure Databricks
- Create an Azure Databricks workspace with connectivity to the blob store.
- Connect this GitHub repo to the Databricks workspace.

### Execute code
- The 'BronzeToSilver' code does some basic transformations and stores the data in delta lake format.
- The 'SilverToGold' code provides samples of filtering, aggregates, etc.