from newsapi import NewsApiClient
import pandas as pd
from datetime import date, timedelta
from newspaper import Article, Config
from nltk.corpus import stopwords
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def extract_transform_function():

    today = date.today()
    yesterday = today - timedelta(days = 1)
    day_before_yesterday = today - timedelta(days = 2)
    # Init
    newsapi = NewsApiClient(api_key='ff4373852c2343a98303951439854f8c')

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