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
    modified :group{
        sec @3 :Int64;
        nsec @4 :UInt32;
    }
    index @5 :Index;
    blocks @6 :List(SourceBlock);
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
        min :group {
            sec @3 :Int64;
            nsec @4 :UInt32;
        }
        max :group {
            sec @5 :Int64;
            nsec @6 :UInt32;
        }
    }
}

# Various flags.
const flagLevelDebug :UInt64    = 0x0000000000000001;
const flagLevelInfo :UInt64     = 0x0000000000000002;
const flagLevelWarning :UInt64  = 0x0000000000000004;
const flagLevelError :UInt64    = 0x0000000000000008;
const flagLevelMask :UInt64     = 0x00000000000000FF;
const flagUnsorted :UInt64      = 0x0000000000000100;
const flagHasTimestamps :UInt64 = 0x0000000000000200;
const flagBinary :UInt64        = 0x8000000000000000;
