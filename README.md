# Discourse-to-CSV
Dump Discourse "North Star" metrics to a SQL compatible CSV. (each row is a date, each column a metric)
<br>
Massive thank you to YoZheng for writing the OG scripts this is based on. (https://github.com/yozheng-afk/discourse_script/)

Often logs dates out of order. But that doesn't affect my use case, so likely wont bother to fix.

Added a "to google sheets" update for the client.py. (use it instead to ship to a google sheets doc. 
