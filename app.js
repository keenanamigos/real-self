import axios from 'axios';
import fs from 'fs';
import { stringify } from 'csv-stringify';
import pLimit from 'p-limit';

const API_URL = 'https://search.realself.com/site_search';
const SIZE = 20;
const TOTAL_RECORDS = 3102; // This can be dynamically gotten in the future
const MAX_CONCURRENT_REQUESTS = 5;
const RETRY_LIMIT = 3;

// Create a write stream for the CSV file
const csvStream = fs.createWriteStream('real_self_data.csv');
const csvStringifier = stringify({
    header: true,
    columns: ['id', 'type', 'answer_count', 'question_body', 'title', 'uri'],
});

// Pipe the stringifier to the file stream
csvStringifier.pipe(csvStream);

async function fetchData(from, attempt = 1) {
    try {
        const response = await axios.get(API_URL, { params: { 
            query: 'mohs%20surgery',
            type: 'question',
            size: SIZE,
            from
        }});
        const contents = response.data.contents || [];

        console.log(`✅ Fetched ${contents.length} records (from=${from})`);

        // Write extracted data to CSV stream
        contents.forEach(({ id, type, answer_count, question_body, title, uri }) => {
            csvStringifier.write({ id, type, answer_count, question_body, title, uri });
        });

        return contents.length;
    } catch (error) {
        console.error(`❌ Error fetching from=${from} (Attempt ${attempt}): ${error.message}`);

        if (attempt < RETRY_LIMIT) {
            const delay = Math.pow(2, attempt) * 1000; // Exponential backoff (1s, 2s, 4s)
            console.log(`🔄 Retrying after ${delay / 1000}s...`);
            await new Promise((res) => setTimeout(res, delay));
            return fetchData(from, attempt + 1);
        } else {
            console.error(`🚨 Failed after ${RETRY_LIMIT} attempts. Skipping from=${from}`);
            return 0;
        }
    }
}

async function run() {
    const limit = pLimit(MAX_CONCURRENT_REQUESTS);
    let tasks = [];

    for (let from = 0; from < TOTAL_RECORDS; from += SIZE) {
        tasks.push(limit(() => fetchData(from)));
    }

    await Promise.all(tasks);

    // Close the CSV stream
    csvStringifier.end();
    console.log('🎉 All records fetched and saved to real_self_data.csv');
}

run();