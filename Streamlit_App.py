from BaseFile import *
import streamlit as st
import time


st.set_page_config(
    page_title="YouTube Channel Data Analysis",
    layout='wide'
    )

st.title(":red[YouTube Channal Data Analysis]")

with st.expander("Channel IDs"):
    channel_id = st.text_input("Enter Channel ID:", key="channel_ids")
    st.info("To enter multiple channel IDs, separate them with commas(,). \n\n"
    "You can get channel ID from the respective channel's about section")
    channel_ids = [cid.strip() for cid in channel_id.split(',')]



with st.expander("API Key"):
    api_key = st.text_input("Enter API Key:", type="password", key="api_key")
    st.info("You can get API Key from Google Cloud Console by creating a project and enabling Youtube API")


if 'step' not in st.session_state:
    st.session_state.step = 0

if st.session_state.step == 0:
    if st.button("Get Channel Info"):
        if api_key and channel_ids:
            with st.spinner("Extracting Channel details from Youtube Data API ....."):
                rawdata = Youtube_API(channel_ids, api_key)
                rawdata_p = playlist_details(channel_ids, api_key)
                st.write(rawdata)
                st.session_state.step = 1

if st.session_state.step == 1:
    if st.button("Store data in MongoDB"):
        with st.spinner("Storing the unstructured data to MongoDB Atlas..."):
            insert_data_to_mongodb(channel_ids, api_key)
            insert_playlist_to_mongodb(channel_ids, api_key)
            time.sleep(2)
        st.success("Uploaded Successfully")
        st.session_state.step = 2

if st.session_state.step == 2:

    if 'button_clicked' not in st.session_state:
        st.session_state.button_clicked = False

    if not st.session_state.button_clicked:
        if st.button('Display DataFrame'):
            with st.spinner("Converting unstructured data into pandas data frame..."):
                get_mongodb_data_as_json()
                get_mongodb_playlistdata_as_json()
                st.session_state.button_clicked = True

    if st.session_state.button_clicked:
        dataframe = ["Select", "Channel Details",
                            "Playlist Details",
                            "Video Details",
                            "Comment Details"
                            ]

        dataframe_selected = st.selectbox("View as Dataframe", options=dataframe)

        if dataframe_selected == "Channel Details":
            ChannelDF = channel_dataframe()
            st.dataframe(ChannelDF)
        elif dataframe_selected == "Playlist Details":
            PlaylistDF = playlist_dataframe()
            st.dataframe(PlaylistDF)
        elif dataframe_selected == "Video Details":
            VideoDF = videos_dataframe()
            st.dataframe(VideoDF)
        elif dataframe_selected == "Comment Details":
            CommentDF = comments_dataframe()
            st.dataframe(CommentDF)

    if st.button("Migrate to Structured Database"):
        with st.spinner("Migrating data to a SQL data warehouse..."):
            try:
                migrate_channel_to_mysql()
            except Exception as e:
                st.error(f"An error occurred while uploading data to MySQL: {str(e)}")
            try:
                migrate_videos_to_mysql()
            except Exception as e:
                st.error(f"An error occurred while uploading data to MySQL: {str(e)}")
            try:
                migrate_comments_to_mysql()
            except Exception as e:
                st.error(f"An error occurred while uploading data to MySQL: {str(e)}")
            try:
                migrate_playlist_to_mysql()
            except Exception as e:
                st.error(f"An error occurred while uploading data to MySQL: {str(e)}")
            st.success("Migration Completed")


    question = ["Select", "What are the names of all the videos and their corresponding channels?",
                        "Which channels have the most number of videos, and how many videos do they have?",
                        "What are the top 10 most viewed videos and their respective channels?",
                        "How many comments were made on each video, and what are their corresponding video names?",
                        "Which videos have the highest number of likes, and what are their corresponding channel names?",
                        "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                        "What is the total number of views for each channel, and what are their corresponding channel names?",
                        "What are the names of all the channels that have published videos in the year 2022?",
                        "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                        "Which videos have the highest number of comments, and what are their corresponding channel names?"
                        ]
    question_selected = st.selectbox("Frequently asked Questions", options=question)

    if question_selected == "What are the names of all the videos and their corresponding channels?":
        st.dataframe(Question1())
    elif question_selected == 'Which channels have the most number of videos, and how many videos do they have?':
        st.dataframe(Question2())
    elif question_selected == 'What are the top 10 most viewed videos and their respective channels?':
        st.dataframe(Question3())
    elif question_selected == 'How many comments were made on each video, and what are their corresponding video names?':
        st.dataframe(Question4())
    elif question_selected == 'Which videos have the highest number of likes, and what are their corresponding channel names?':
        st.dataframe(Question5()) 
    elif question_selected == 'What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.dataframe(Question6())  
    elif question_selected == 'What is the total number of views for each channel, and what are their corresponding channel names?':
        st.dataframe(Question7())
    elif question_selected == "What are the names of all the channels that have published videos in the year 2022?":
        st.dataframe(Question8())
    elif question_selected == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        st.dataframe(Question9())
    elif question_selected == "Which videos have the highest number of comments, and what are their corresponding channel names?":
        st.dataframe(Question10())