import { SQL } from "bun";

/**
 * Mengimpor isi file SQL ke database yang terhubung.
 * @param filePath - Jalur ke file .sql yang akan diimpor.
 * @param sqlClient - Instance SQL dari Bun.
 * @param options - Opsi tambahan untuk proses impor.
 */
export async function importSqlFile(filePath: string, sqlClient: SQL, options: { noOwner: boolean, skipExisting: boolean, truncateTable: boolean, skipTables: string[] } = { noOwner: false, skipExisting: false, truncateTable: false, skipTables: [] }) {
  const file = Bun.file(filePath);
  if (!(await file.exists())) {
    throw new Error(`File not found: ${filePath}`);
  }

  // Menjalankan isi file SQL dengan streaming per line
  const textDecoder = new TextDecoder();
  let stream = file.stream();
  
  if (filePath.endsWith('.gz')) {
    stream = stream.pipeThrough(new DecompressionStream("gzip"));
  }
  
  const reader = stream.getReader();
  const BATCH_SIZE = 4096;
  let buffer = "";
  let currentQuery = "";
  let copyBuffer: string[] = [];
  let isCopyMode = false;
  let copyCommand = "";
  let currentCopyTable = "";
  let queryCount = 0;

  // Ensure all skipTables are fully qualified if no schema is present.
  const normalizedSkipTables = options.skipTables.map(t => t.includes('.') ? t : `public.${t}`);

  console.log(`Executing SQL Import from file: ${filePath} (Streaming)${options.noOwner ? ' (Skipping OWNER TO)' : ''}${options.skipExisting ? ' (Skipping Existing)' : ''}${options.truncateTable ? ' (Truncating Tables)' : ''}${options.skipTables.length > 0 ? ` (Skipping Tables: ${options.skipTables.join(',')})` : ''}`);

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += textDecoder.decode(value);

    let newlineIndex;
    while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, newlineIndex).trim();
      buffer = buffer.slice(newlineIndex + 1);

      if (isCopyMode) {
        if (line === '\\.') {
          // End of COPY block, execute remaining buffer
          if (!normalizedSkipTables.includes(currentCopyTable.includes('.') ? currentCopyTable : `public.${currentCopyTable}`) && copyBuffer.length > 0) {
            await executeCopy(sqlClient, copyCommand, copyBuffer);
          }
          isCopyMode = false;
          copyBuffer = [];
          copyCommand = "";
          currentCopyTable = "";
        } else {
          copyBuffer.push(line);
          if (copyBuffer.length >= BATCH_SIZE) {
            if (!normalizedSkipTables.includes(currentCopyTable.includes('.') ? currentCopyTable : `public.${currentCopyTable}`)) {
                await executeCopy(sqlClient, copyCommand, copyBuffer);
            }
            copyBuffer = [];
          }
        }
        continue;
      }

      if (line.startsWith('--') || line.startsWith('\\') || line.length === 0 || (options.noOwner && line.includes('OWNER TO'))) {
        continue;
      }
      
      if (line.startsWith('COPY ')) {
        const tableMatch = line.match(/COPY\s+(\S+)\s*\(([^)]+)\)/);
        const tableName = tableMatch ? tableMatch[1] : "";
        isCopyMode = true;
        copyCommand = line;
        currentCopyTable = tableName;
        continue;
      }

      currentQuery += (currentQuery.length > 0 ? " " : "") + line;

      if (line.endsWith(';')) {
        queryCount++;
        const queryToExecute = currentQuery.slice(0, -1).trim(); // Remove trailing semicolon
        
        let shouldSkip = false;
        if (queryToExecute.toUpperCase().startsWith('CREATE TABLE')) {
            const tableNameMatch = queryToExecute.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)\s*\(/i);
            if (tableNameMatch) {
                const tableName = tableNameMatch[1].includes('.') ? tableNameMatch[1] : `public.${tableNameMatch[1]}`;
                if (normalizedSkipTables.includes(tableName)) {
                    shouldSkip = true;
                    console.log(`[Query ${queryCount}] Skipping CREATE TABLE: ${tableNameMatch[1]}`);
                }
            }
        }

        if (!shouldSkip) {
            console.log(`[Query ${queryCount}] Executing: ${queryToExecute.substring(0, 100).replace(/\n/g, ' ')}...`);
            try {
              await sqlClient.unsafe(queryToExecute);
              
              if (options.truncateTable && queryToExecute.toUpperCase().startsWith('CREATE TABLE')) {
                  const tableNameMatch = queryToExecute.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)\s*\(/i);
                  if (tableNameMatch) {
                      const tableName = tableNameMatch[1].includes('.') ? tableNameMatch[1] : `public.${tableNameMatch[1]}`;
                      console.log(`[Query ${queryCount}] Truncating table: ${tableName}`);
                      await sqlClient.unsafe(`TRUNCATE TABLE ${tableName};`);
                  }
              }
            } catch (err: any) {
              const errorMsg = (err.message || '').toLowerCase();
              const isAlreadyExists = 
                  (err.code === '42P07' || err.code === '42710' || err.code === '23505' || err.code === '42P16') || 
                  (errorMsg.includes('already exists') || 
                   errorMsg.includes('could not create') || 
                   errorMsg.includes('duplicate key value') ||
                   errorMsg.includes('multiple primary keys'));

              if (options.skipExisting && isAlreadyExists) {
                console.log(`[Query ${queryCount}] Skipped (Already exists or constraint violation)`);
              } else {
                throw err;
              }
            }
        }
        currentQuery = "";
      }
    }
  }

  // Handle remaining query if the file doesn't end with a newline/semicolon
  if (currentQuery.trim().length > 0) {
    queryCount++;
    const queryToExecute = currentQuery.trim().endsWith(';') ? currentQuery.trim().slice(0, -1) : currentQuery.trim();
    console.log(`[Query ${queryCount}] Executing (remaining): ${queryToExecute.substring(0, 100).replace(/\n/g, ' ')}...`);
    try {
      await sqlClient.unsafe(queryToExecute);
    } catch (err: any) {
      const isAlreadyExists = (err.code === '42P07' || err.code === '42710') || (err.message && err.message.toLowerCase().includes('already exists'));
      if (options.skipExisting && isAlreadyExists) {
        console.log(`[Query ${queryCount}] Skipped (Already exists)`);
      } else {
        throw err;
      }
    }
  }
}

async function executeCopy(sqlClient: SQL, copyCommand: string, data: string[]) {
  // Convert COPY command to INSERT batch
  const tableMatch = copyCommand.match(/COPY\s+(\S+)\s*\(([^)]+)\)/);
  if (!tableMatch) return; // Simple parser, might need improvement for complex COPY
  
  const tableName = tableMatch[1];
  const columns = tableMatch[2].split(',').map(c => c.trim());
  const numColumns = columns.length;
  
  const values = data.map(line => {
    const fields = line.split('\t');
    
    // Ensure the number of fields matches columns, pad with NULL if necessary
    const paddedFields = fields.slice(0, numColumns);
    while (paddedFields.length < numColumns) {
        paddedFields.push('\\N');
    }

    return '(' + paddedFields.map(f => f === '\\N' ? 'NULL' : `'${f.replace(/'/g, "''")}'`).join(', ') + ')';
  }).join(', ');
  
  const insertSql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES ${values};`;
  try {
      await sqlClient.unsafe(insertSql);
      console.log(`[COPY -> INSERT] Batched ${data.length} rows into ${tableName}`);
  } catch (err: any) {
      console.error(`[COPY -> INSERT] FAILED to batch insert into ${tableName}. Columns: ${numColumns}. SQL snippet: ${insertSql.substring(0, 100)}...`);
      throw err;
  }
}
