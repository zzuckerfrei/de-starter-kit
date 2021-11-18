# 월별마다 액티브한 사용자들의 수

usc = %sql SELECT * FROM raw_data.user_session_channel
usc_df = usc.DataFrame()
usc_df.head()


st = %sql SELECT * FROM raw_data.session_timestamp
st_df = st.DataFrame()
st_df.head()


total_df = usc_df.merge(st_df, on='sessionid')
total_df.head()


total_df['date'] = total_df['ts'].apply(lambda x: "%d-%02d-%02d" % (x.year, x.month, x.day))
total_df['date_ym'] = total_df['ts'].apply(lambda x: "%d-%02d" % (x.year, x.month))
total_df.head()

# date_ym컬럼을 기준으로 그룹화 시킨 후, userid 컬럼의 유일한 값을 카운트함
total_df.groupby('date_ym')['userid'].nunique()