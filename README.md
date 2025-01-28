# AnimeDataInsights

AnimeDataInsights is a project aimed at extracting, processing, analysing, and visualising anime data to uncover insights into anime genres, studios, and viewer preferences. This project gets it data from the MyAnimeList.net API, processes it through various data processing techniques, and finally uses tableau for visualisation. 

# Project Structure

MyAnime_API.py: This script is responsible for fetching anime data from the API. It handles API requests and data extraction to collect anime metadata for processing and analysis. All data was extracted to a local CSV. 

create_raw_data_tbl.py: This script creates a MYSQL table from the CSV file. It stores the anime data in a suitable place for analysis and visualization tasks.

studio_performance_analysis.py: This is where the the data analysis and visualisation took place. Before analysis, a table called "perf_an" was created to make this more efficient; the synopses and title columns were omitted. Analyses include, bar plots, time series analysis, heatmaps and so on. 

Synopses.py: Focuses on cleaning and processing the anime synopses for the NLP analysis. It involves text manipulation techniques to prepare the data. Then a table to store these synopses called "processed_synopsis" were also created. This was to organise the data efficiently for analysis. 

Genre_Classification.py: This script was dedicated at an attempt to classifying anime into genres. It applies a random forest analysis to categorise anime based on five genres, which aids in the analysis of genre popularity and trends. While the outcome was not successful, it was still a good learning experience. 

# Visualisation

The project uses Tableau for data visualizsation. The visualisation can be found here: https://public.tableau.com/views/AnimeAnalysis_17095682924010/Dashboard1?:language=en-GB&:sid=&:display_count=n&:origin=viz_share_link



