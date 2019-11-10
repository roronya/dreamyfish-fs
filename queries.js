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
    RANK() OVER(PARTITION BY games.id ORDER BY candidates.tokuten DESC) AS rank
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
  *
FROM
  candidates
WHERE
  rank <= 5
`
module.exports = {gamesQuery, recommendsQuery};
