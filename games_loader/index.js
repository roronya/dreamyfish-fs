// Import the Google Cloud client library using default credentials
const {BigQuery} = require('@google-cloud/bigquery');
const admin = require('firebase-admin');
const CHUNK_SIZE = 100

const gamesQuery = `
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
`

const main = async () => {
    // initialize
    const bigquery = new BigQuery();
    admin.initializeApp({
    credential: admin.credential.applicationDefault()
    });
    const db = admin.firestore();

    const [job] = await bigquery.createQueryJob({ query: gamesQuery, location: 'US'})
    console.log(`Job ${job.id} started.`)
    // Jobと同様
    const [games] = await job.getQueryResults()
    console.log(`Jobs ${job.id} ended.`)
    const gamesCollectionRef = db.collection('games')
    console.log('games inserting started.')
    for (let i=0;i<games.length;i+=CHUNK_SIZE) {
        console.log(`chunk ${i}:${i+CHUNK_SIZE} started.`)
        const promises = games.slice(i, i+CHUNK_SIZE).map(game => gamesCollectionRef.add(game))
        await Promise.all(promises)
        console.log(`chunk ${i}:${i+CHUNK_SIZE} ended.`)
    }
    console.log('games inserting ended.')
} 

main().then(() => console.log("done"))
