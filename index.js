// Import the Google Cloud client library using default credentials
const {BigQuery} = require('@google-cloud/bigquery');
const admin = require('firebase-admin');
const {gamesQuery, recommendsQuery} = require('./queries')
const CHUNK_SIZE = 100

const main = async () => {
    // initialize
    const bigquery = new BigQuery();
    admin.initializeApp({
    credential: admin.credential.applicationDefault()
    });
    const db = admin.firestore();

    const gamesJob = bigquery.createQueryJob({ query: gamesQuery, location: 'US'})
    const recommendsJob = bigquery.createQueryJob({ query: recommendsQuery, location: 'US'})
    // 2つのJobを並列で投げたいので、それぞれでawaitしないで、Promise.allにする
    // Jobは配列で帰るが一番最初の要素しか使わないので[0]する
    const jobs = (await Promise.all([gamesJob, recommendsJob])).map(job => job[0])
    const jobIds = jobs.map(job => job.id)
    console.log(`Jobs ${jobIds} started.`)
    // Jobと同様
    const [games, recommends] = (await Promise.all(jobs.map(job => job.getQueryResults()))).map(result => result[0])
    console.log(`Jobs ${jobIds} ended.`)
    const gamesCollectionRef = db.collection('games')
    const recommendsCollectionRef = db.collection('recommends')
    // Firestoreへ並列で書き込むために、Promiseを先に作って最後にPromise.allで待ち合わせる
    // const gamePromises = games.map(game => gamesCollectionRef.add(game))
    // Promise.all(gamePromises)
    console.log('games inserting started.')
    for (let i=0;i<games.length;i+=CHUNK_SIZE) {
        console.log(`chunk ${i}:${i+CHUNK_SIZE} started.`)
        const promises = games.slice(i, i+CHUNK_SIZE).map(game => gamesCollectionRef.add(game))
        await Promise.all(promises)
        console.log(`chunk ${i}:${i+CHUNK_SIZE} ended.`)
    }
    console.log('games inserting ended.')
    // const recommendPromises = recommends.map(recommend => recommendsCollectionRef.add(recommend))
    // Promise.all(recommendPromises)
    console.log('recommends inserting started.')
    for (let i=0;i<recommends.length;i+=CHUNK_SIZE) {
        console.log(`chunk ${i}:${i+CHUNK_SIZE} started.`)
        const promises = recommends.map(recommend => recommendsCollectionRef.add(recommend))
        await Promise.all(promises)
        console.log(`chunk ${i}:${i+CHUNK_SIZE} ended.`)
    }
    console.log('recommends inserting ended.')
} 

main().then(() => console.log("done"))
