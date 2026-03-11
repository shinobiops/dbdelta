import pg from 'pg';
const { Client } = pg;

const ADMIN_URL = process.env.DBDELTA_TEST_ADMIN_URL || 'postgres://postgres:postgres@localhost:5433/postgres';
const FROM_DB = 'dbdelta_test_from';
const TO_DB = 'dbdelta_test_to';

export function getTestUrls() {
  const base = ADMIN_URL.replace(/\/[^/]*$/, '');
  return {
    fromUrl: `${base}/${FROM_DB}`,
    toUrl: `${base}/${TO_DB}`,
  };
}

export async function setupTestDatabases() {
  const admin = new Client({ connectionString: ADMIN_URL });
  await admin.connect();
  try {
    for (const db of [FROM_DB, TO_DB]) {
      await admin.query(`
        select pg_terminate_backend(pid)
        from pg_stat_activity
        where datname = '${db}' and pid <> pg_backend_pid()
      `);
      await admin.query(`drop database if exists ${db}`);
      await admin.query(`create database ${db}`);
    }
  } finally {
    await admin.end();
  }
  return getTestUrls();
}

export async function teardownTestDatabases() {
  const admin = new Client({ connectionString: ADMIN_URL });
  await admin.connect();
  try {
    for (const db of [FROM_DB, TO_DB]) {
      await admin.query(`
        select pg_terminate_backend(pid)
        from pg_stat_activity
        where datname = '${db}' and pid <> pg_backend_pid()
      `);
      await admin.query(`drop database if exists ${db}`);
    }
  } finally {
    await admin.end();
  }
}

export async function connectTo(url) {
  const client = new Client({ connectionString: url });
  await client.connect();
  return client;
}
