import http from 'k6/http';
import redis from "k6/x/redis";
import { sleep } from 'k6';
import { SharedArray } from 'k6/data';

export const options = {
  // env: { REDISEARCH_ADDRS: 'localhost:26379' },
  stages: [
    { duration: '30s', target: 100 }, // traffic ramp-up from 1 to 100 users
    { duration: '2m', target: 100 }, // stay at 100 users for 2 minutes
    { duration: '30s', target: 0 }, // ramp-down to 0 users
  ],
};

const data = new SharedArray('news', function () {
  const f = open('../test_data/News_Category_Dataset_v3.json')
  const words = f.split("\n")
  .filter(line => line.length > 0)
  .map(line => JSON.parse(line))
  .map(v => ({
    link: v.link,
    headline: v.headline,
    short_description: v.short_description
  }))
  .map(v => {
    const str = v.headline + " " + v.short_description
    return Array.from(str.matchAll(/[A-Za-z]+/g), m => m[0])
  }).reduce((prev, cur) => {prev.push(...cur); return prev})
  return Array.from(new Set(words))
})

// Instantiate a new redis client
const redisClient = new redis.Client({
  addrs: __ENV.REDIS_ADDRS ?! __ENV.REDIS_ADDRS.split(",") : new Array("localhost:6379"),
  password: __ENV.REDIS_PASSWORD || "",
});

const rediSearchClient = new redis.Client({
  addrs: __ENV.REDISEARCH_ADDRS ?! __ENV.REDISEARCH_ADDRS.split(",") : new Array("localhost:16379"),
  password: __ENV.REDISEARCH_PASSWORD || "",
})

export default async function () {
  const word1 = data[Math.floor(Math.random() * data.length)]
  const word2 = data[Math.floor(Math.random() * data.length)]
  const result = await rediSearchClient.sendCommand('FT.SEARCH', 'idx', `${word1} ${word2}`, 'LIMIT', 0, 20)
}
