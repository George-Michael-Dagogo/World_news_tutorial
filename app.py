import pandas as pd
import pyarrow.parquet as pq
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import seaborn as sns
from pages import recent_data, overall_data
from azure.storage.blob import BlobServiceClient, BlobClient 

connection_string = ""
container_name = "testtech"
local_download_path = "./blob_files"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)
blob_list = container_client.list_blobs()

for blob in blob_list:
    blob_client = container_client.get_blob_client(blob.name)
    local_file_path = os.path.join(local_download_path,blob.name)

    with open(local_file_path, 'wb') as f:
        download_stream = blob_client.download_blob()
        f.write(download_stream.read())
        print(f'Downloaded blob: {blob.name}')

st.set_page_config(page_title="Two Days Late News Dashboard", page_icon=":bar_chart:", layout="wide")
st.subheader("Hi, I am Michael and this is my Streamlit app :wave:")




# Streamlit app title
st.title("News Sentiment Analysis")

# Navigation using Streamlit's sidebar
st.sidebar.title("Navigation")
page_names = ["Most Recent Data", "Overall Data"]
page_selection = st.sidebar.selectbox("Select a page", page_names)

# Display the selected page content based on user selection
if page_selection == "Most Recent Data":
    recent_data.show()
elif page_selection == "Overall Data":
    overall_data.show()
else:
    st.error("Please select a page.")