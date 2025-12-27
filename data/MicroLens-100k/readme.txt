In "MicroLens-100k_pairs.tsv", each row corresponds to a user ID, and the subsequent columns represent the video IDs that
the user interacted with in chronological order. We use the "\t" delimiter to separate the user ID from the first video ID,
and a single space to separate different video IDs within the same row.

Additionally, we also provide the interaction file for MicroLens-100k (namely "MicroLens-100k_pairs.csv"), in which 
the three columns are "userID", "videoID" and "timestamp".

In "MicroLens-100k_comments_en.txt", the three columns are "userID", "videoID", "commentContent", split by "\t".

# Top 50 videos phổ biến nhất
python scripts/download_videos.py --mode popular --num 50

# Hoặc tải thêm 20 videos mẫu
python scripts/download_videos.py --mode sample --num 20