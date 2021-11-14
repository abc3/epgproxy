
const { Pool, Client } = require('pg')
var faker = require('faker')

const pool = new Pool({
  host: "localhost",
  port: 5555,
  database: "postgres",
  user: "postgres",
  password: "postgres",
})

export default async (req, res) => {
  const q = await pool.query("select 1;")
  res.json(JSON.stringify(q.rows))
}
