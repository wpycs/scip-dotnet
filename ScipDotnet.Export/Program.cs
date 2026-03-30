using Google.Protobuf;
using Scip;
using ScipDotnet.Export;

if (args.Length < 1)
{
    Console.Error.WriteLine("Usage: scip-export <input.db> [output.scip]");
    Console.Error.WriteLine();
    Console.Error.WriteLine("Reconstructs a standard SCIP protobuf index file from a SQLite database.");
    return 1;
}

var inputDb = args[0];
var outputScip = args.Length >= 2 ? args[1] : "index.scip";

if (!File.Exists(inputDb))
{
    Console.Error.WriteLine($"Error: database file not found: {inputDb}");
    return 1;
}

Console.Error.WriteLine($"Reading: {inputDb}");

using var reader = new SqliteIndexReader(inputDb);

var metadata = new Metadata
{
    Version = ProtocolVersion.UnspecifiedProtocolVersion,
    ToolInfo = new ToolInfo
    {
        Name = "scip-export",
        Version = "0.1.0"
    },
    TextDocumentEncoding = TextEncoding.Utf8
};

var documentCount = 0;
using (var fileStream = File.Create(outputScip))
{
    var cos = new CodedOutputStream(fileStream, leaveOpen: true);

    // Field 1: Metadata
    cos.WriteTag(1, WireFormat.WireType.LengthDelimited);
    cos.WriteMessage(metadata);
    cos.Flush();

    // Field 2: Documents (streamed one at a time)
    foreach (var doc in reader.ReadDocuments())
    {
        cos.WriteTag(2, WireFormat.WireType.LengthDelimited);
        cos.WriteMessage(doc);
        cos.Flush();
        documentCount++;
    }
}

Console.Error.WriteLine($"Done: wrote {documentCount} documents to {outputScip}");
return 0;
