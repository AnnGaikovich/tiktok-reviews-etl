# TikTok Reviews Processing: Airflow + MongoDB

This project automatically processes TikTok reviews from Google Play: checks the file, cleans data, calculates dates, and loads the result into MongoDB. Everything runs in Docker.

## What's used

- Airflow (workflow management)
- Python (pandas, pymongo)
- MongoDB (database)
- Docker Compose (orchestration)

## How it works

### First DAG (process_tiktok_data)

1. Waits for the reviews file (`tiktok_google_play_reviews.csv`).
2. Checks if the file is empty. If empty – logs a message and stops.
3. If the file contains data, runs a chain of actions:
   - reads the CSV
   - replaces empty values with a dash `-` (leaves the date column untouched)
   - sorts records by date (column `at`)
   - cleans the `content` column by removing extra characters (keeps only letters, digits, and punctuation)
4. Saves the processed data to `processed_tiktok.csv`.

### Second DAG (load_to_mongodb)

1. Waits for the `processed_tiktok.csv` file to appear.
2. Loads it into MongoDB (collection `reviews`).

## How to run locally

1. **Clone the repository**  
   ```bash
   git clone https://github.com/AnnGaikovich/tiktok-reviews-etl.git
   ```

2. **Place the reviews file**  
   - Create a `data` folder in the project root  
   - Copy `tiktok_google_play_reviews.csv` into it

3. **Start the containers**  
   ```bash
   docker compose up -d
   ```  

4. **Configure MongoDB connection in Airflow**  
   - Open your browser: `http://localhost:8080`  
   - Login: `airflow`, password: `airflow`  
   - Go to `Admin` → `Connections` → `Add connection`  
   - Fill in the fields:  
     - `Connection Id`: `mongodb_default`  
     - `Connection Type`: `MongoDB`  
     - `Host`: `host.docker.internal` (if MongoDB is on your computer)  
     - `Port`: `27017`  
     - `Database`: `tiktok`  
   - Click `Save`.

5. **Turn on both DAGs**.

6. **Trigger the first DAG**.

7. **The second DAG** will automatically load data into MongoDB as soon as it sees the file.

## Results – verify in MongoDB Compass

Connect to MongoDB via Compass: `localhost:27017`, database `tiktok`, collection `reviews`.  
Run the three queries (copy and paste each one).

1. Most frequent comments (top 5)

[
  { $group: { _id: "$content", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 5 }
]

2. Short comments (shorter than 5 characters)

[
  { $group: { _id: { $dateToString: { format: "%Y-%m-%d", date: "$at" } }, avg_rating: { $avg: "$score" } } },
  { $project: { date: { $toDate: "$_id" }, avg_rating: 1, _id: 0 } },
  { $sort: { date: 1 } }
]

3. Average rating per day (timestamp format)

[
  { $group: { _id: { $dateToString: { format: "%Y-%m-%d", date: "$at" } }, avg_rating: { $avg: "$score" } } },
  { $project: { date: { $toDate: "$_id" }, avg_rating: 1, _id: 0 } },
  { $sort: { date: 1 } }
]