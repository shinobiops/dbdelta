import pg from 'pg';
const { Client } = pg;

export async function connect(connectionString) {
  const client = new Client({ connectionString });
  await client.connect();
  return client;
}
