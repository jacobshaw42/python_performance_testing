# Python Dataframe and SQL Server Insert Performance Testing

After spending sometime researching what methods in python are the best to transformation dataframes or ingest data into a SQL Server database, I noticed some conflicting information. 

Counter to my initial assumptions, I even found information suggesting that a iterrows over a dataframe would basically be the same thing as an apply method. 

The following are the results of some basic tests I performed several times. 

The file used for testing was a TSV from PatentsView (g_application.tsv) that is 8433988 records, 6 columns, and roughly 370 MB unzipped.

# Setup

You will need need to install the packages in the `requirements.txt` file

You can use  the `get_docker.sh` to setup a SQL Server docker container, if necessary

Your machine running the `test_inserts.py` will also need to have SQL Server's `bcp` command line tool accessible.

