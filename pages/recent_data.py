import pandas as pd
import pyarrow.parquet as pq
import os
import streamlit as st
import pandas as pd
import plotly.express as px
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import plotly.graph_objects as go
from wordcloud import WordCloud
import nltk


# Function to extract date from filename
def extract_date(filename):
    return pd.to_datetime(filename.split('.')[0].split('news-')[1])

# Path to the folder containing parquet files
folder_path = "./blob_files"
# Get list of parquet files in the folder
parquet_files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]

# Extract dates from filenames
dates = [extract_date(f) for f in parquet_files]

# Find the latest date
latest_date_index = dates.index(max(dates))

# Read the parquet file with the latest date into a DataFrame
latest_dataframe = pd.read_parquet(os.path.join(folder_path, parquet_files[latest_date_index]))
nltk.download('stopwords')
nltk.download('punkt')
def show():
    # Title and description for the recent data page
    st.subheader("Most Recent Data")
    st.write("This page displays insights from the most recently added news articles.")

    sentiment_data = latest_dataframe.Sentiment.value_counts()
    sentiment_data = sentiment_data.reset_index()
    fig = px.bar(sentiment_data, x='Sentiment',y= 'count',color = 'Sentiment', title='Sentiment Distribution',color_discrete_map = {'Positive': 'green', 'Negative': 'red', 'Neutral': 'gray'})

    source_data = latest_dataframe.source.value_counts()
    source_data = source_data.reset_index()
    source_data = source_data.head(10)
    fig2 = px.bar(source_data, x = 'source', y = 'count', title= 'Top Recent Sources')

    text_data = ' '.join(latest_dataframe.content.astype(str).tolist())
    tokens = word_tokenize(text_data)
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word.lower() not in stop_words]
    filtered_text = ' '.join(filtered_tokens)
    wordcloud = WordCloud(width=1000, height=500, background_color='white').generate(filtered_text)
    fig3 = go.Figure()
    fig3.add_trace(go.Image(z=wordcloud.to_array()))
    fig3.update_layout(title='Word Cloud for Recent Data')





    # Display the bar chart using Streamlit
    st.plotly_chart(fig)
    st.plotly_chart(fig2)
    st.plotly_chart(fig3)
    