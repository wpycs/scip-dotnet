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
    private readonly Dictionary<string, long> _symbolCache = new();
    private readonly HashSet<string> _seenDocuments = new();

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
            "CREATE TABLE IF NOT EXISTS documents (id INTEGER PRIMARY KEY, language TEXT, relative_path TEXT NOT NULL UNIQUE, position_encoding TEXT, text TEXT);" +
            "CREATE TABLE IF NOT EXISTS chunks (id INTEGER PRIMARY KEY, document_id INTEGER NOT NULL, chunk_index INTEGER NOT NULL, start_line INTEGER NOT NULL, end_line INTEGER NOT NULL, occurrences BLOB NOT NULL, FOREIGN KEY (document_id) REFERENCES documents(id));" +
            "CREATE TABLE IF NOT EXISTS global_symbols (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL UNIQUE, display_name TEXT, kind INTEGER, documentation TEXT, signature BLOB, enclosing_symbol TEXT, relationships BLOB);" +
            "CREATE TABLE IF NOT EXISTS mentions (chunk_id INTEGER NOT NULL, symbol_id INTEGER NOT NULL, role INTEGER NOT NULL, PRIMARY KEY (chunk_id, symbol_id, role), FOREIGN KEY (chunk_id) REFERENCES chunks(id), FOREIGN KEY (symbol_id) REFERENCES global_symbols(id));" +
            "CREATE TABLE IF NOT EXISTS defn_enclosing_ranges (id INTEGER PRIMARY KEY, document_id INTEGER NOT NULL, symbol_id INTEGER NOT NULL, start_line INTEGER NOT NULL, start_char INTEGER NOT NULL, end_line INTEGER NOT NULL, end_char INTEGER NOT NULL, FOREIGN KEY (document_id) REFERENCES documents(id), FOREIGN KEY (symbol_id) REFERENCES global_symbols(id));";
        cmd.ExecuteNonQuery();
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
        return id;
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
        _compressor.Dispose();
        _transaction.Dispose();
        _connection.Dispose();
    }
}
