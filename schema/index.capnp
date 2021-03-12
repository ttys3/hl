@0xb4c004ef430e6d9f;

# Root is the root entity of index file.
struct Root {
    source @0 :SourceFile;
}

# Source contains metadata of scanned source log file.
struct SourceFile {
    size @0 :UInt64;
    sha256 @1 :Data;
    path @2 :Text;
    modified @3 :Timestamp;
    index @4 :Index;
    blocks @5 :List(SourceBlock);
}

# Block is an information about a part of source log file.
struct SourceBlock {
    offset @0 :UInt64;
    size @1 :UInt64;
    index @2 :Index;
}

# Index holds index information of a block or a whole file.
struct Index {
    flags @0 :UInt64;
    lines :group{
        valid @1 :UInt64;
        invalid @2 :UInt64;
    }
    timestamps :group {
        present @3 :Bool;
        min @4 :Timestamp;
        max @5 :Timestamp;
    }
}

# Various flags.
const flagLevelMask :UInt64    = 0x00000000000000FF;
const flagLevelDebug :UInt64   = 0x0000000000000001;
const flagLevelInfo :UInt64    = 0x0000000000000002;
const flagLevelWarning :UInt64 = 0x0000000000000004;
const flagLevelError :UInt64   = 0x0000000000000008;
const flagSorted :UInt64       = 0x0000000000000100;
const flagBinary :UInt64       = 0x8000000000000000;

# Timestamp is a Unix timestamp in millisecond precision (milliseconds elapsed since Jan 1 1970).
using Timestamp = Int64;

# Magic holds some simple signature to quickly detect if the file content is valid.
using Magic = UInt64;
