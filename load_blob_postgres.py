from newsapi import NewsApiClient
import pandas as pd
from newspaper import Article, Config
from nltk.corpus import stopwords
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

import pyarrow as pa 
import pyarrow.parquet as pq
from io import BytesIO
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
from datetime import date, timedelta

import psycopg2
from sqlalchemy import create_engine

def extract_transform_function():

    today = date.today()
    yesterday = today - timedelta(days = 1)
    day_before_yesterday = today - timedelta(days = 2)
    # Init
    newsapi = NewsApiClient(api_key='yourkey')

    # /v2/top-headlines
    top_headlines = newsapi.get_top_headlines(   
                                            category='entertainment',
                                            language='en',
                                            page_size = 90,
                                            page= 1)

    articles = top_headlines.get('articles',[])

    init_df = pd.DataFrame(articles, columns = ['source','title','publishedAt','author','url'])

    init_df['source'] = init_df['source'].apply(lambda x: x['name'] if pd.notna(x) and 'name' in x else None)

    init_df['publishedAt'] = pd.to_datetime(init_df['publishedAt'])



    filtered_df = init_df[(init_df['publishedAt'].dt.date == day_before_yesterday) | (init_df['publishedAt'].dt.date == yesterday)]


    df = filtered_df.copy()

    def full_content(url):
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
        config = Config()
        config.browser_user_agent = user_agent
        page = Article(url, config = config)

        try:
            page.download()
            page.parse()
            return page.text
        except Exception as e:
            print(f"Error retrieving content from {url}: {e}")
            return 'couldnt retrieve'


    df['content'] = df['url'].apply(full_content)
    df['content'] = df['content'].str.replace('\n', ' ')
    df = df[df['content'] != 'couldnt retrieve']



    # Download the stopwords dataset
    nltk.download('stopwords')
    nltk.download('punkt')
    nltk.download('wordnet')


    def count_words_without_stopwords(text):
        if isinstance(text, (str, bytes)):
            words = nltk.word_tokenize(str(text))
            stop_words = set(stopwords.words('english'))
            filtered_words = [word for word in words if word.lower() not in stop_words]
            return len(filtered_words)
        else:
            0

    df['word_count'] = df['content'].apply(count_words_without_stopwords)




    nltk.download('vader_lexicon')

    sid = SentimentIntensityAnalyzer()

    def get_sentiment(row):
        sentiment_scores = sid.polarity_scores(row)
        compound_score = sentiment_scores['compound']

        if compound_score >= 0.05:
            sentiment = 'Positive'
        elif compound_score <= -0.05:
            sentiment = 'Negative'
        else:
            sentiment = 'Neutral'

        return sentiment, compound_score

    df[['Sentiment', 'Compound_Score']] = df['content'].astype(str).apply(lambda x: pd.Series(get_sentiment(x)))

    return df

dataframe = extract_transform_function()
dataframe = dataframe

today = date.today()
day_before_yesterday = today - timedelta(days = 2)

def load_to_blob(df):
    table = pa.Table.from_pandas(df)
    parquet_buffer = BytesIO()
    pq.write_table(table,parquet_buffer)

    connection_string = ''
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_name = 'testtech'
    blob_name = f'news-{day_before_yesterday}.parquet'
    container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(parquet_buffer.getvalue(),overwrite = True)

    print(f'{blob_name} successfully uploaded')


def load_to_postgres(df):
    try:
        
        # Create a dictionary of database connection parameters
        connection_params = {
            "host": "testtech.postgres.database.azure.com",
            "port": "5432",
            "user": "testtech",
            "password": "",
            "database": "postgres"
        }

        # Create a SQLAlchemy engine for connecting to the database
        engine = create_engine(
            f'postgresql+psycopg2://{connection_params["user"]}:{connection_params["password"]}@{connection_params["host"]}:{connection_params["port"]}/{connection_params["database"]}'
        )

        # Append the DataFrame contents to the existing table
        df.to_sql("news_table", engine, if_exists='append', index=False)

        print('Database successfully updated')

    except Exception as e:
        print("An error occurred:", e)
        # Optionally: Log the error for further analysis

    finally:
        # Close the database connection gracefully
        if engine:
            engine.dispose()



load_to_blob(dataframe)


