import { SQL } from "bun";
import { importSqlFile } from "./psql-import.ts";

async function getPasswordFromPgPass(host: string, user: string) {
  const pgpassPath = process.env.PGPASSFILE;
  if (!pgpassPath) {
    console.error("psql: error: PGPASSFILE environment variable is not set.");
    process.exit(1);
  }
  const file = Bun.file(pgpassPath);
  if (!(await file.exists())) return '';

  const content = await file.text();
  const lines = content.split('\n');

  for (const line of lines) {
    const cleanLine = line.replace('\r', '').trim();
    if (cleanLine.startsWith("#") || !cleanLine) continue;
    const parts = cleanLine.split(":");
    if (parts.length < 5) continue;

    const [h, p, db, u, pass] = parts;
    if ((h === "*" || h === host) && (u === "*" || u === user)) {
      return pass;
    }
  }
  return '';
}
export async function run(host: string, user: string, sqlCommand: string | null, importFile: string | null, noOwner: boolean, skipExisting: boolean, truncateTable: boolean, skipTables: string[], dbName: string | null) {
  const password = await getPasswordFromPgPass(host, user);
  if (!password) {
    // console.error(`psql: error: No credentials found for ${user}@${host} in PGPASSFILE`);
    // process.exit(1);
  }
  // Connect to the database, default to 'postgres' if not provided
  const targetDb = dbName || process.env.PGDATABASE || "postgres";
  console.log(`Connecting to database: ${targetDb}`);
  const connectionString = `postgres://${user}:${encodeURIComponent(password)}@${host}:5432/${targetDb}`;
  const sql = new SQL(connectionString);
  try {
    if (importFile) {
        const start = performance.now();
        await importSqlFile(importFile, sql, { noOwner, skipExisting, truncateTable, skipTables });
        const end = performance.now();
        const duration = end - start;
        let formattedDuration = "";
        if (duration < 1000) {
            formattedDuration = `${duration.toFixed(2)} ms`;
        } else if (duration < 60000) {
            formattedDuration = `${(duration / 1000).toFixed(2)} s`;
        } else {
            formattedDuration = `${(duration / 60000).toFixed(2)} m`;
        }
        console.log(`Import completed successfully in ${formattedDuration}.`);
    } else if (sqlCommand) {
        console.log(`Executing SQL: ${sqlCommand}`);
        const result = await sql.unsafe(sqlCommand);
        console.table(result);
    }
  } catch (err: any) {
    if (err.code === '28P01') {
      console.error(`psql: error: password authentication failed for user "${user}"`);
    } else {
      console.error(`psql: error: ${err.message}`);
    }
    process.exit(1);
  } finally {
    await sql.close();
  }
}

if (require.main === module) {
  const args = Bun.argv.slice(2);
  const noOwner = args.includes("--no-owner");
  const skipExisting = args.includes("--skip-existing");
  const truncateTable = args.includes("--truncate-table");
  
  const skipTableIndex = args.indexOf("--skip-table");
  const skipTables = skipTableIndex !== -1 ? args[skipTableIndex + 1].split(',') : [];

  // Filter out flags from args to find host/user/command/file indices correctly
  const filteredArgs = args.filter(a => a !== "--no-owner" && a !== "--skip-existing" && a !== "--truncate-table" && a !== "--skip-table" && (skipTableIndex === -1 || a !== args[skipTableIndex + 1]));
  const hostIndex = filteredArgs.indexOf("-h");
  const host = hostIndex !== -1 ? filteredArgs[hostIndex + 1] : "localhost";
  const userIndex = filteredArgs.indexOf("-U");
  const user = userIndex !== -1 ? filteredArgs[userIndex + 1] : "postgres";
  const dbIndex = filteredArgs.indexOf("-d");
  const dbName = dbIndex !== -1 ? filteredArgs[dbIndex + 1] : null;
  const commandIndex = filteredArgs.indexOf("-c");
  const sqlCommand = commandIndex !== -1 ? filteredArgs[commandIndex + 1] : null;
  const importIndex = filteredArgs.indexOf("-i");
  const importFile = importIndex !== -1 ? filteredArgs[importIndex + 1] : null;

  if (!sqlCommand && !importFile) {
    console.error("Usage: psql -h <host> -U <user> [-d <database>] (-c <sql_command> | -i <sql_file>) [--no-owner] [--skip-existing] [--truncate-table] [--skip-table table1,table2]");
    process.exit(1);
  }
  run(host, user, sqlCommand, importFile, noOwner, skipExisting, truncateTable, skipTables, dbName);
}
