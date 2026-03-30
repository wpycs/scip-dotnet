using Google.Protobuf;
using Microsoft.Data.Sqlite;
using Scip;
using ZstdSharp;

namespace ScipDotnet.Export;

/// <summary>
/// Reads a SQLite index database produced by scip-dotnet and reconstructs
/// standard SCIP protobuf Document and SymbolInformation objects.
/// </summary>
public sealed class SqliteIndexReader : IDisposable
{
    private readonly SqliteConnection _connection;
    private readonly Decompressor _decompressor;

    public SqliteIndexReader(string dbPath)
    {
        _connection = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly");
        _connection.Open();
        _decompressor = new Decompressor();
    }

    /// <summary>
    /// Streams all documents from the database, fully reconstructed with occurrences and per-document symbols.
    /// </summary>
    public IEnumerable<Document> ReadDocuments()
    {
        // Load all global symbols into a lookup for enriching occurrences
        var symbolInfoMap = LoadSymbolInfo();

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT id, language, relative_path FROM documents ORDER BY id;";
        using var reader = cmd.ExecuteReader();

        while (reader.Read())
        {
            var docId = reader.GetInt64(0);
            var language = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var relativePath = reader.GetString(2);

            var doc = new Document
            {
                Language = language,
                RelativePath = relativePath
            };

            // Read and decompress all chunks for this document
            var docSymbols = new HashSet<string>();
            foreach (var occ in ReadOccurrences(docId))
            {
                doc.Occurrences.Add(occ);
                if (!string.IsNullOrEmpty(occ.Symbol))
                    docSymbols.Add(occ.Symbol);
            }

            // Attach SymbolInformation for symbols that have definitions in this document
            foreach (var sym in docSymbols)
            {
                if (symbolInfoMap.TryGetValue(sym, out var info))
                    doc.Symbols.Add(info);
            }

            yield return doc;
        }
    }

    /// <summary>
    /// Reads all global SymbolInformation entries (for external_symbols in the SCIP Index).
    /// </summary>
    public List<SymbolInformation> ReadAllSymbolInfo()
    {
        return LoadSymbolInfo().Values.ToList();
    }

    private Dictionary<string, SymbolInformation> LoadSymbolInfo()
    {
        var map = new Dictionary<string, SymbolInformation>();
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT symbol, display_name, documentation, relationships FROM global_symbols;";
        using var reader = cmd.ExecuteReader();

        while (reader.Read())
        {
            var symbol = reader.GetString(0);
            var info = new SymbolInformation { Symbol = symbol };

            if (!reader.IsDBNull(1))
                info.Documentation.Add(reader.GetString(1)); // display_name as first doc entry

            if (!reader.IsDBNull(2))
            {
                // documentation was stored as joined string (skip(1) lines)
                var docText = reader.GetString(2);
                if (!string.IsNullOrEmpty(docText))
                {
                    foreach (var line in docText.Split('\n'))
                        info.Documentation.Add(line);
                }
            }

            if (!reader.IsDBNull(3))
            {
                var relsBlob = (byte[])reader.GetValue(3);
                if (relsBlob.Length > 0)
                {
                    // Writer uses rel.WriteTo(ms) for each relationship — raw message
                    // bytes concatenated without length prefixes. Since protobuf merges
                    // repeated writes of the same message type, we parse the entire blob
                    // as a single Relationship (matching the writer's serialization).
                    var rel = new Relationship();
                    rel.MergeFrom(relsBlob);
                    info.Relationships.Add(rel);
                }
            }

            map[symbol] = info;
        }

        return map;
    }

    private IEnumerable<Occurrence> ReadOccurrences(long docId)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT occurrences FROM chunks WHERE document_id = $docId ORDER BY chunk_index;";
        cmd.Parameters.AddWithValue("$docId", docId);
        using var reader = cmd.ExecuteReader();

        while (reader.Read())
        {
            var compressed = (byte[])reader.GetValue(0);
            var decompressed = _decompressor.Unwrap(compressed).ToArray();
            // WriteMessage writes: varint length + message bytes (no field tag)
            var offset = 0;
            while (offset < decompressed.Length)
            {
                var cis = new CodedInputStream(decompressed, offset, decompressed.Length - offset);
                var length = cis.ReadLength();
                var headerSize = CodedOutputStream.ComputeLengthSize(length);
                var msgBytes = new byte[length];
                Array.Copy(decompressed, offset + headerSize, msgBytes, 0, length);
                var occ = Occurrence.Parser.ParseFrom(msgBytes);
                yield return occ;
                offset += headerSize + length;
            }
        }
    }

    public void Dispose()
    {
        _decompressor.Dispose();
        _connection.Dispose();
    }
}
