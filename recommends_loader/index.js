// Import the Google Cloud client library using default credentials
const {BigQuery} = require('@google-cloud/bigquery');
const admin = require('firebase-admin');
const CHUNK_SIZE = 100

const recommendsQuery = `
WITH
  games AS (
  SELECT
    id
  FROM
    \`erscape-recommender-system.erscape.gamelist\` ),
  reviews AS (
  SELECT
    game AS game_id,
    uid AS user_id,
    tokuten
  FROM
    \`erscape-recommender-system.erscape.userreview\`),
  candidates AS (
  SELECT
    games.id,
    candidates.game_id,
    candidates.tokuten,
    ROW_NUMBER() OVER(PARTITION BY games.id ORDER BY candidates.tokuten DESC) AS rank
  FROM
    games
  INNER JOIN
    reviews
  ON
    games.id = reviews.game_id
  INNER JOIN
    reviews AS candidates
  ON
    reviews.user_id = candidates.user_id
  WHERE
    candidates.tokuten >= 70 )
SELECT
  id,
  ARRAY_AGG(game_id) AS recommends
FROM
  candidates
WHERE
  rank <= 5
GROUP BY
  id
`

const main = async () => {
    // initialize
    const bigquery = new BigQuery();
    admin.initializeApp({
    credential: admin.credential.applicationDefault()
    });
    const db = admin.firestore();

    const [job] = await bigquery.createQueryJob({ query: recommendsQuery, location: 'US'})
    console.log(`Jobs ${job.id} started.`)
    const [recommends] = await job.getQueryResults()
    console.log(`Jobs ${job.id} ended.`)
    const recommendsCollectionRef = db.collection('recommends')
    console.log('recommends inserting started.')
    for (let i=0;i<recommends.length;i+=CHUNK_SIZE) {
        console.log(`chunk ${i}:${i+CHUNK_SIZE} started.`)
        const promises = recommends.slice(i, i+CHUNK_SIZE).map(recommend => recommendsCollectionRef.add(recommend))
        await Promise.all(promises)
        console.log(`chunk ${i}:${i+CHUNK_SIZE} ended.`)
    }
    console.log('recommends inserting ended.')
} 

main().then(() => console.log("done"))
