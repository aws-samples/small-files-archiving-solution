
# error 'inotify watch limit reached'

sudo sysctl fs.inotify.max_user_watches=524288
sudo sysctl -p

# run streamlit
streamlit run archiver-web.py 