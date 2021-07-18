CREATE TABLE TelecomUserDataAnalysis
(
    bearer_id FLOAT PRIMARY KEY,
    Start TEXT NOT NULL,
    Start_ms FLOAT NOT NULL,
    end TEXT NOT NULL,
    end_ms FLOAT NOT NULL,
    IMSI FLOAT NOT NULL,
    MSISDN_Number  FLOAT NOT NULL,
    IMEI FLOAT NOT NULL,
    Last_Location_Name  VARCHAR(50) NOT NULL,
    Avg_RTT_DL_ms FLOAT NOT NULL,
    Avg_RTT_UL_ms FLOAT NOT NULL,
    TCP_DL_Retrans_Vol_Bytes FLOAT NOT NULL,
    TCP_UL_Retrans_Vol_Bytes FLOAT NOT NULL,
    Avg_Bearer_TP_DL_kbps FLOAT NOT NULL,
    Avg_Bearer_TP_UL_kbps FLOAT NOT NULL,
    Handset_Manufacturer VARCHAR(100) NOT NULL,
    Handset_Type VARCHAR(100) NOT NULL,
    Social_Media_DL_Bytes FLOAT NOT NULL,
    Social_Media_UL_Bytes FLOAT NOT NULL,
    social_media_data FLOAT NOT NULL,
    google_data FLOAT NOT NULL,
    email_data FLOAT NOT NULL,
    youtube_data FLOAT NOT NULL,
    netflix_data FLOAT NOT NULL,
    gaming_data FLOAT NOT NULL,
    total_dl_ul FLOAT NOT NULL,
    RTT_DL_UL FLOAT NOT NULL,
    TCP_DL_UL FLOAT NOT NULL,
    TP_DL_UL FLOAT NOT NULL
)
-- ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;