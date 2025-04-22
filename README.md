# Miniature-Redis-Clone (On progress!)

<img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/redis/redis-original.svg"  height="250" />
<img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" height="250" />
<header>Redis is an In-memory database, that uses the Main memory and cache to store Data instead of The Hard disc, Redis is widely used in Backend servers for Caching but also as a main database.</header>
<br/>
<details>
  <summary>Core Features</summary>
  <ul>
    <li>Each Key can have a TTL (Time to Live) in The Map </li>
    <li>Supports Lists data structure with TTL as well</li>
    <li>You can even Queuing commands as a batch and executing it.</li>
  </ul>
</details>

<h1>Test screen shots! </h1>
<div>
  <p>So for testing two values name and age were set with ttl of k seconds as shown. after those k seconds the values were removed from the map!</p>
  <img src="/montages/TTL-test-with-map.png" height="300" alt="Testing hashmap of redis with TTL" />
  <img src="/montages/server-runtime.png" height="300" alt="Server runtime screen shot!" />
</div>

<h3>Run the script with: </h3>
<p>server script</p>
<code>
user$: python3 app/main.py
</code>

<p>testing client script</p>
<code>
user$: python3 app/client.py
</code>
