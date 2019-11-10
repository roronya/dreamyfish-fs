// Import the Google Cloud client library using default credentials
const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();

const gameQuery = `
SELECT
  game.id,
  amazon.largeimage AS image_url,
  game.gamename AS name
FROM
  \`erscape-recommender-system.erscape.gamelist\` AS game
INNER JOIN
  \`erscape-recommender-system.erscape.amazon_game\` AS amazon_game
ON
  game.id = amazon_game.game
INNER JOIN
  \`erscape-recommender-system.erscape.amazonlist\` AS amazon
ON
  amazon_game.asin = amazon.asin
limit 10
`

const query = async () => {
  // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
  const options = {
    query: gameQuery,
    // Location must match that of the dataset(s) referenced in the query.
    location: 'US',
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);
  console.log(`Job ${job.id} started.`);

  // Wait for the query to finish
  const [rows] = await job.getQueryResults();

  // Print the results
  console.log('Rows:');
  rows.forEach(row => console.log(row));
  return rows
}

const main = async () => {
    const games = await query()
} 

main().then(() => console.log("done"))
