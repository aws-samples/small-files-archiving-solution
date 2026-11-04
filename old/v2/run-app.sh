#!/bin/bash
#streamlit run archiver-web.py --server.fileWatcherType none
# loopback only: the app has no authentication. From another machine use an SSH
# tunnel (ssh -L 8501:127.0.0.1:8501 host) instead of a wider bind.
streamlit run app.py --server.fileWatcherType none --server.address 127.0.0.1
