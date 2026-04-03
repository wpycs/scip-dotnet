using Google.Protobuf;
using Microsoft.Data.Sqlite;
using Scip;
using ZstdSharp;

namespace ScipDotnet;

public sealed class SqliteIndexWriter : IDisposable
{
    private const int ChunkLineSize = 100;
    private const int ZstdCompressionLevel = 3;
    private readonly SqliteConnection _connection;
    private readonly SqliteTransaction _transaction;
    private readonly Compressor _compressor;
    private readonly SqliteCommand _insertDocument;
    private readonly SqliteCommand _insertChunk;
    private readonly SqliteCommand _insertGlobalSymbol;
    private readonly SqliteCommand _insertMention;
    private readonly SqliteCommand _insertDefnRange;
    private readonly SqliteCommand _lookupSymbol;
    private readonly SqliteCommand _lookupDocument;
    private readonly SqliteCommand _updateDocumentHash;
    private readonly Dictionary<string, string?> _contentHashCache = new();
    private readonly SqliteCommand _purgeDocMentions;
    private readonly SqliteCommand _purgeDocDefnRanges;
    private readonly SqliteCommand _purgeDocChunks;
    private readonly SqliteCommand _purgeDocument;
    private readonly SqliteCommand _insertInheritance;
    private readonly Dictionary<string, long> _symbolCache = new();
    private readonly HashSet<string> _seenDocuments = new();

    /// <summary>
    /// Tracks how many documents were skipped because their content hash was unchanged.
    /// </summary>
    public int SkippedCount { get; private set; }

    /// <summary>
    /// Tracks how many stale documents were purged (deleted from disk).
    /// </summary>
    public int PurgedCount { get; private set; }

    public SqliteIndexWriter(string dbPath)
    {
        var fullPath = Path.GetFullPath(dbPath);
        var dir = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
        {
            Directory.CreateDirectory(dir);
        }
        _compressor = new Compressor(ZstdCompressionLevel);
        _connection = new SqliteConnection($"Data Source={fullPath}");
        _connection.Open();
        using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-64000;";
            cmd.ExecuteNonQuery();
        }
        _transaction = _connection.BeginTransaction();
        CreateSchema();
        _insertDocument = Prepare("INSERT OR IGNORE INTO documents (language, relative_path) VALUES ($lang, $path);", ("$lang", SqliteType.Text), ("$path", SqliteType.Text));
        _insertChunk = Prepare("INSERT INTO chunks (document_id, chunk_index, start_line, end_line, occurrences) VALUES ($docId, $idx, $start, $end, $occ) RETURNING id;", ("$docId", SqliteType.Integer), ("$idx", SqliteType.Integer), ("$start", SqliteType.Integer), ("$end", SqliteType.Integer), ("$occ", SqliteType.Blob));
        _insertGlobalSymbol = Prepare("INSERT OR IGNORE INTO global_symbols (symbol, display_name, documentation, relationships) VALUES ($sym, $display, $doc, $rels);", ("$sym", SqliteType.Text), ("$display", SqliteType.Text), ("$doc", SqliteType.Text), ("$rels", SqliteType.Blob));
        _insertMention = Prepare("INSERT OR IGNORE INTO mentions (chunk_id, symbol_id, role) VALUES ($chunkId, $symId, $role);", ("$chunkId", SqliteType.Integer), ("$symId", SqliteType.Integer), ("$role", SqliteType.Integer));
        _insertDefnRange = Prepare("INSERT INTO defn_enclosing_ranges (document_id, symbol_id, start_line, start_char, end_line, end_char) VALUES ($docId, $symId, $sl, $sc, $el, $ec);", ("$docId", SqliteType.Integer), ("$symId", SqliteType.Integer), ("$sl", SqliteType.Integer), ("$sc", SqliteType.Integer), ("$el", SqliteType.Integer), ("$ec", SqliteType.Integer));
        _lookupSymbol = Prepare("SELECT id FROM global_symbols WHERE symbol = $sym;", ("$sym", SqliteType.Text));
        _lookupDocument = Prepare("SELECT id FROM documents WHERE relative_path = $path;", ("$path", SqliteType.Text));
        _updateDocumentHash = Prepare("UPDATE documents SET content_hash = $hash WHERE relative_path = $path;", ("$hash", SqliteType.Text), ("$path", SqliteType.Text));
        _insertInheritance = Prepare("INSERT OR IGNORE INTO inheritance_chains (symbol_id, base_symbol_id, depth) VALUES ($symId, $baseSymId, $depth);", ("$symId", SqliteType.Integer), ("$baseSymId", SqliteType.Integer), ("$depth", SqliteType.Integer));
        LoadContentHashCache();
        _purgeDocMentions = Prepare("DELETE FROM mentions WHERE chunk_id IN (SELECT id FROM chunks WHERE document_id = $docId);", ("$docId", SqliteType.Integer));
        _purgeDocDefnRanges = Prepare("DELETE FROM defn_enclosing_ranges WHERE document_id = $docId;", ("$docId", SqliteType.Integer));
        _purgeDocChunks = Prepare("DELETE FROM chunks WHERE document_id = $docId;", ("$docId", SqliteType.Integer));
        _purgeDocument = Prepare("DELETE FROM documents WHERE id = $docId;", ("$docId", SqliteType.Integer));
    }

    private SqliteCommand Prepare(string sql, params (string name, SqliteType type)[] ps)
    {
        var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (name, type) in ps) cmd.Parameters.Add(name, type);
        cmd.Prepare();
        return cmd;
    }

    private void CreateSchema()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE IF NOT EXISTS documents (id INTEGER PRIMARY KEY, language TEXT, relative_path TEXT NOT NULL UNIQUE, position_encoding TEXT, text TEXT, content_hash TEXT);" +
            "CREATE TABLE IF NOT EXISTS chunks (id INTEGER PRIMARY KEY, document_id INTEGER NOT NULL, chunk_index INTEGER NOT NULL, start_line INTEGER NOT NULL, end_line INTEGER NOT NULL, occurrences BLOB NOT NULL, FOREIGN KEY (document_id) REFERENCES documents(id));" +
            "CREATE TABLE IF NOT EXISTS global_symbols (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL UNIQUE, display_name TEXT, kind INTEGER, documentation TEXT, signature BLOB, enclosing_symbol TEXT, relationships BLOB);" +
            "CREATE TABLE IF NOT EXISTS mentions (chunk_id INTEGER NOT NULL, symbol_id INTEGER NOT NULL, role INTEGER NOT NULL, PRIMARY KEY (chunk_id, symbol_id, role), FOREIGN KEY (chunk_id) REFERENCES chunks(id), FOREIGN KEY (symbol_id) REFERENCES global_symbols(id));" +
            "CREATE TABLE IF NOT EXISTS defn_enclosing_ranges (id INTEGER PRIMARY KEY, document_id INTEGER NOT NULL, symbol_id INTEGER NOT NULL, start_line INTEGER NOT NULL, start_char INTEGER NOT NULL, end_line INTEGER NOT NULL, end_char INTEGER NOT NULL, FOREIGN KEY (document_id) REFERENCES documents(id), FOREIGN KEY (symbol_id) REFERENCES global_symbols(id));" +
            "CREATE TABLE IF NOT EXISTS inheritance_chains (id INTEGER PRIMARY KEY, symbol_id INTEGER NOT NULL, base_symbol_id INTEGER NOT NULL, depth INTEGER NOT NULL DEFAULT 1, FOREIGN KEY (symbol_id) REFERENCES global_symbols(id), FOREIGN KEY (base_symbol_id) REFERENCES global_symbols(id), UNIQUE(symbol_id, base_symbol_id));";
        cmd.ExecuteNonQuery();
        // Migrate: add content_hash column if missing (for existing databases)
        MigrateAddContentHash();
    }

    private void MigrateAddContentHash()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "PRAGMA table_info(documents);";
        using var reader = cmd.ExecuteReader();
        var hasContentHash = false;
        while (reader.Read())
        {
            if (string.Equals(reader.GetString(1), "content_hash", StringComparison.OrdinalIgnoreCase))
            {
                hasContentHash = true;
                break;
            }
        }
        reader.Close();
        if (!hasContentHash)
        {
            using var alter = _connection.CreateCommand();
            alter.CommandText = "ALTER TABLE documents ADD COLUMN content_hash TEXT;";
            alter.ExecuteNonQuery();
        }
    }

    private void LoadContentHashCache()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT relative_path, content_hash FROM documents;";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var path = reader.GetString(0);
            var hash = reader.IsDBNull(1) ? null : reader.GetString(1);
            _contentHashCache[path] = hash;
        }
    }

    /// <summary>
    /// Returns true if the document needs to be re-indexed (hash mismatch or not yet indexed).
    /// </summary>
    public bool ShouldReindex(string relativePath, string contentHash)
    {
        if (_contentHashCache.TryGetValue(relativePath, out var cachedHash))
        {
            return !string.Equals(cachedHash, contentHash, StringComparison.Ordinal);
        }
        return true;
    }

    /// <summary>
    /// Purges all indexed data for a document (chunks, mentions, defn_ranges) so it can be re-indexed.
    /// </summary>
    public void PurgeDocument(string relativePath)
    {
        _lookupDocument.Parameters["$path"].Value = relativePath;
        var result = _lookupDocument.ExecuteScalar();
        if (result is null or DBNull) return;
        var docId = (long)result;
        PurgeDocumentById(docId);
    }

    private void PurgeDocumentById(long docId)
    {
        _purgeDocMentions.Parameters["$docId"].Value = docId;
        _purgeDocMentions.ExecuteNonQuery();
        _purgeDocDefnRanges.Parameters["$docId"].Value = docId;
        _purgeDocDefnRanges.ExecuteNonQuery();
        _purgeDocChunks.Parameters["$docId"].Value = docId;
        _purgeDocChunks.ExecuteNonQuery();
    }

    /// <summary>
    /// Removes documents from the database that are no longer present on disk.
    /// </summary>
    public void PurgeDeletedFiles(HashSet<string> currentFiles)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT id, relative_path FROM documents;";
        var toDelete = new List<(long id, string path)>();
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var path = reader.GetString(1);
                if (!currentFiles.Contains(path))
                    toDelete.Add((reader.GetInt64(0), path));
            }
        }
        foreach (var (id, _) in toDelete)
        {
            PurgeDocumentById(id);
            _purgeDocument.Parameters["$docId"].Value = id;
            _purgeDocument.ExecuteNonQuery();
        }
        PurgedCount = toDelete.Count;
    }

    /// <summary>
    /// Marks a document as skipped (unchanged) during incremental indexing.
    /// </summary>
    public void MarkSkipped()
    {
        SkippedCount++;
    }

    /// <summary>
    /// Updates the stored content hash for a document after successful re-indexing.
    /// </summary>
    public void UpdateContentHash(string relativePath, string contentHash)
    {
        _updateDocumentHash.Parameters["$hash"].Value = contentHash;
        _updateDocumentHash.Parameters["$path"].Value = relativePath;
        _updateDocumentHash.ExecuteNonQuery();
        _contentHashCache[relativePath] = contentHash;
    }

    private long EnsureSymbol(string symbol, SymbolInformation? info = null)
    {
        if (_symbolCache.TryGetValue(symbol, out var cached)) return cached;
        string? displayName = null, documentation = null;
        byte[]? relsBlob = null;
        if (info != null)
        {
            if (info.Documentation.Count > 0) displayName = info.Documentation[0];
            if (info.Documentation.Count > 1) documentation = string.Join("\n", info.Documentation.Skip(1));
            if (info.Relationships.Count > 0)
            {
                using var ms = new MemoryStream();
                foreach (var rel in info.Relationships) rel.WriteTo(ms);
                relsBlob = ms.ToArray();
            }
        }
        _insertGlobalSymbol.Parameters["$sym"].Value = symbol;
        _insertGlobalSymbol.Parameters["$display"].Value = (object?)displayName ?? DBNull.Value;
        _insertGlobalSymbol.Parameters["$doc"].Value = (object?)documentation ?? DBNull.Value;
        _insertGlobalSymbol.Parameters["$rels"].Value = (object?)relsBlob ?? DBNull.Value;
        _insertGlobalSymbol.ExecuteNonQuery();
        _lookupSymbol.Parameters["$sym"].Value = symbol;
        var id = (long)_lookupSymbol.ExecuteScalar()!;
        _symbolCache[symbol] = id;

        // Write inheritance relationships (is_implementation => base class / interface)
        if (info != null)
        {
            foreach (var rel in info.Relationships)
            {
                if (!rel.IsImplementation || string.IsNullOrEmpty(rel.Symbol)) continue;
                if (IsImplicitBaseType(rel.Symbol)) continue;
                var baseId = EnsureSymbol(rel.Symbol);
                _insertInheritance.Parameters["$symId"].Value = id;
                _insertInheritance.Parameters["$baseSymId"].Value = baseId;
                _insertInheritance.Parameters["$depth"].Value = 1;
                _insertInheritance.ExecuteNonQuery();
            }
        }

        return id;
    }

    private static readonly HashSet<string> ImplicitBaseTypes = new(StringComparer.Ordinal)
    {
        // Implicit base classes
        "Object", "System/Object",
        "ValueType", "System/ValueType",
        "Enum", "System/Enum",
        // Interfaces implicitly implemented by System.Enum / value types
        "IFormattable", "System/IFormattable",
        "IComparable", "System/IComparable",
        "IConvertible", "System/IConvertible",
        "ISpanFormattable", "System/ISpanFormattable",
        "IUtf8SpanFormattable", "System/IUtf8SpanFormattable",
        // Delegate implicit base
        "Delegate", "System/Delegate",
        "MulticastDelegate", "System/MulticastDelegate",
    };

    private static bool IsImplicitBaseType(string symbol)
    {
        // SCIP symbols: "scip-dotnet nuget <pkg> <ver> System/Object#"
        // We match the trailing type descriptor (name + '#') against known implicit bases
        var hashIdx = symbol.LastIndexOf('#');
        if (hashIdx <= 0) return false;
        // Find the start of the type name (after last '/' or last ' ')
        var nameStart = symbol.LastIndexOfAny(new[] { '/', ' ' }, hashIdx - 1);
        if (nameStart < 0) return false;
        var typeName = symbol.Substring(nameStart + 1, hashIdx - nameStart - 1);
        if (ImplicitBaseTypes.Contains(typeName)) return true;
        // Also check with namespace prefix: e.g. "System/Object"
        var nsStart = symbol.LastIndexOf(' ', nameStart - 1);
        if (nsStart >= 0)
        {
            var qualifiedName = symbol.Substring(nsStart + 1, hashIdx - nsStart - 1);
            if (ImplicitBaseTypes.Contains(qualifiedName)) return true;
        }
        return false;
    }

    public void WriteDocument(Document doc)
    {
        var path = doc.RelativePath ?? "";
        if (!_seenDocuments.Add(path)) return; // skip duplicate document

        _insertDocument.Parameters["$lang"].Value = doc.Language ?? "";
        _insertDocument.Parameters["$path"].Value = path;
        _insertDocument.ExecuteNonQuery();
        _lookupDocument.Parameters["$path"].Value = path;
        var docId = (long)_lookupDocument.ExecuteScalar()!;
        var symbolInfoMap = new Dictionary<string, SymbolInformation>();
        foreach (var sym in doc.Symbols) symbolInfoMap.TryAdd(sym.Symbol, sym);
        var sorted = doc.Occurrences.Where(o => o.Range.Count >= 3).OrderBy(o => o.Range[0]).ToList();
        if (sorted.Count == 0) return;
        var chunks = ChunkOccurrences(sorted);
        var chunkIdx = 0;
        foreach (var (startLine, endLine, occs) in chunks)
        {
            var compressed = _compressor.Wrap(SerializeOccurrences(occs)).ToArray();
            _insertChunk.Parameters["$docId"].Value = docId;
            _insertChunk.Parameters["$idx"].Value = chunkIdx;
            _insertChunk.Parameters["$start"].Value = startLine;
            _insertChunk.Parameters["$end"].Value = endLine;
            _insertChunk.Parameters["$occ"].Value = compressed;
            var chunkId = (long)_insertChunk.ExecuteScalar()!;
            foreach (var occ in occs)
            {
                if (string.IsNullOrEmpty(occ.Symbol)) continue;
                symbolInfoMap.TryGetValue(occ.Symbol, out var info);
                var symbolId = EnsureSymbol(occ.Symbol, info);
                var isDef = (occ.SymbolRoles & (int)SymbolRole.Definition) != 0;
                _insertMention.Parameters["$chunkId"].Value = chunkId;
                _insertMention.Parameters["$symId"].Value = symbolId;
                _insertMention.Parameters["$role"].Value = 0;
                _insertMention.ExecuteNonQuery();
                if (isDef)
                {
                    _insertMention.Parameters["$role"].Value = 1;
                    _insertMention.ExecuteNonQuery();
                    if (occ.EnclosingRange.Count >= 4)
                    {
                        _insertDefnRange.Parameters["$docId"].Value = docId;
                        _insertDefnRange.Parameters["$symId"].Value = symbolId;
                        _insertDefnRange.Parameters["$sl"].Value = occ.EnclosingRange[0];
                        _insertDefnRange.Parameters["$sc"].Value = occ.EnclosingRange[1];
                        _insertDefnRange.Parameters["$el"].Value = occ.EnclosingRange[2];
                        _insertDefnRange.Parameters["$ec"].Value = occ.EnclosingRange[3];
                        _insertDefnRange.ExecuteNonQuery();
                    }
                }
            }
            chunkIdx++;
        }
    }

    private static List<(int startLine, int endLine, List<Occurrence> occs)> ChunkOccurrences(List<Occurrence> sorted)
    {
        var result = new List<(int, int, List<Occurrence>)>();
        var current = new List<Occurrence>();
        int chunkStart = 0, chunkEnd = 0;
        foreach (var occ in sorted)
        {
            var line = occ.Range[0];
            if (current.Count == 0) { chunkStart = line; chunkEnd = line; }
            if (line - chunkStart >= ChunkLineSize && current.Count > 0)
            {
                result.Add((chunkStart, chunkEnd, current));
                current = new List<Occurrence>();
                chunkStart = line; chunkEnd = line;
            }
            chunkEnd = Math.Max(chunkEnd, occ.Range.Count == 4 ? occ.Range[2] : occ.Range[0]);
            current.Add(occ);
        }
        if (current.Count > 0) result.Add((chunkStart, chunkEnd, current));
        return result;
    }

    private static byte[] SerializeOccurrences(List<Occurrence> occs)
    {
        using var ms = new MemoryStream();
        using var cos = new CodedOutputStream(ms, leaveOpen: true);
        foreach (var occ in occs) cos.WriteMessage(occ);
        cos.Flush();
        return ms.ToArray();
    }

    public void FinalizeIndex()
    {
        _transaction.Commit();
        using var cmd = _connection.CreateCommand();
        cmd.CommandText =
            "CREATE INDEX IF NOT EXISTS idx_chunks_line_range ON chunks(document_id, start_line, end_line);" +
            "CREATE INDEX IF NOT EXISTS idx_chunks_doc_id ON chunks(document_id);" +
            "CREATE INDEX IF NOT EXISTS idx_mentions_symbol_id_role ON mentions(symbol_id, role);" +
            "CREATE INDEX IF NOT EXISTS idx_defn_enclosing_ranges_symbol_id ON defn_enclosing_ranges(symbol_id);" +
            "CREATE INDEX IF NOT EXISTS idx_defn_enclosing_ranges_document ON defn_enclosing_ranges(document_id, start_line, end_line);" +
            "CREATE INDEX IF NOT EXISTS idx_global_symbols_symbol ON global_symbols(symbol);" +
            "CREATE INDEX IF NOT EXISTS idx_inheritance_symbol ON inheritance_chains(symbol_id);" +
            "CREATE INDEX IF NOT EXISTS idx_inheritance_base ON inheritance_chains(base_symbol_id);" +
            "CREATE VIRTUAL TABLE IF NOT EXISTS global_symbols_fts USING fts5(symbol, content='global_symbols', content_rowid='id', tokenize='trigram');" +
            "INSERT INTO global_symbols_fts(global_symbols_fts) VALUES('rebuild');";
        cmd.ExecuteNonQuery();
    }

    public void Dispose()
    {
        _insertDocument.Dispose();
        _insertChunk.Dispose();
        _insertGlobalSymbol.Dispose();
        _insertMention.Dispose();
        _insertDefnRange.Dispose();
        _lookupSymbol.Dispose();
        _lookupDocument.Dispose();
        _updateDocumentHash.Dispose();
        _purgeDocMentions.Dispose();
        _purgeDocDefnRanges.Dispose();
        _purgeDocChunks.Dispose();
        _purgeDocument.Dispose();
        _insertInheritance.Dispose();
        _compressor.Dispose();
        _transaction.Dispose();
        _connection.Dispose();
    }
}
