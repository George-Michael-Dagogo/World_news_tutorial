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


directory = "../World_news_tutorial/blob_files"

# List all Parquet files in the directory
parquet_files = [file for file in os.listdir(directory) if file.endswith('.parquet')]

# Read each Parquet file into a pandas DataFrame
dfs = []
for file in parquet_files:
    file_path = os.path.join(directory, file)
    df = pd.read_parquet(file_path)
    dfs.append(df)

# Concatenate all DataFrames into a single DataFrame
combined_df = pd.concat(dfs, ignore_index=True)

def show():
    # Title and description for the recent data page
    st.subheader("Overall Data")
    st.write("This page displays insights from the all available news articles.")

    sentiment_data = combined_df.Sentiment.value_counts()
    sentiment_data = sentiment_data.reset_index()
    fig = px.bar(sentiment_data, x='Sentiment',y= 'count',color = 'Sentiment', title='Sentiment Distribution',color_discrete_map = {'Positive': 'green', 'Negative': 'red', 'Neutral': 'gray'})


    source_data = combined_df.source.value_counts()
    source_data = source_data.reset_index()
    source_data = source_data.head(10)
    fig2 = px.bar(source_data, x = 'source', y = 'count', title= 'Top Overall Sources')

    
    text_data = ' '.join(combined_df.content.astype(str).tolist())
    tokens = word_tokenize(text_data)
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word.lower() not in stop_words]
    filtered_text = ' '.join(filtered_tokens)
    wordcloud = WordCloud(width=1000, height=500, background_color='white').generate(filtered_text)
    fig3 = go.Figure()
    fig3.add_trace(go.Image(z=wordcloud.to_array()))
    fig3.update_layout(title='Word Cloud for Overall Data')


    combined_df['publishedAt'] = pd.to_datetime(combined_df['publishedAt']).dt.date
    sentiment_counts = combined_df.groupby(['publishedAt', 'Sentiment'])['Sentiment'].count().unstack(fill_value=0)
    fig4 = px.bar(sentiment_counts, x=sentiment_counts.index, y=sentiment_counts.columns, title='Sentiment Distribution by Day',color_discrete_map = {'Positive': 'green', 'Negative': 'red', 'Neutral': 'gray'})
    fig4.update_layout(xaxis_title='Date', yaxis_title='Average Sentiment Score')
    fig.update_layout(barmode='stack') 

    # Display the bar chart using Streamlit
    st.plotly_chart(fig)
    st.plotly_chart(fig2)
    st.plotly_chart(fig3)
    st.plotly_chart(fig4)
