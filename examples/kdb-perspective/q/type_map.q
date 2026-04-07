// type_map.q — KDB+ type character → Perspective ColumnType mapping
//
// Perspective ColumnType enum (from perspective.proto):
//   STRING   = 0
//   DATE     = 1
//   DATETIME = 2
//   INTEGER  = 3
//   FLOAT    = 4
//   BOOLEAN  = 5

// Map from KDB+ type code (short, as returned by `type each col`) to
// Perspective ColumnType int.
//
// KDB+ type codes (negative = atom, positive = vector):
//   1  boolean    b
//   4  byte       x   (treat as integer)
//   5  short      h
//   6  int        i
//   7  long       j   (→ FLOAT: no 64-bit integer in Perspective)
//   8  real       e
//   9  float      f
//   10 char       c   (→ STRING)
//   11 symbol     s
//   12 timestamp  p   (ns since 2000-01-01 → DATETIME)
//   13 month      m   (→ DATE)
//   14 date       d
//   15 datetime   z   (→ DATETIME)
//   16 timespan   n   (ns → DATETIME)
//   17 minute     u   (→ INTEGER)
//   18 second     v   (→ INTEGER)
//   19 time       t   (ms → INTEGER)
//   20+ enum      *   (→ STRING: resolve symbol)

// Primary mapping: type code (positive, i.e. vector type) → ColumnType int
.psp.t.typeMap: 0N!0N;   // re-assigned below to avoid parse ambiguity

.psp.t.typeMap: (`long$()) ! (`int$());   // empty typed dict
.psp.t.typeMap[1j]:  5i;   // boolean  → BOOLEAN
.psp.t.typeMap[4j]:  3i;   // byte     → INTEGER
.psp.t.typeMap[5j]:  3i;   // short    → INTEGER
.psp.t.typeMap[6j]:  3i;   // int      → INTEGER
.psp.t.typeMap[7j]:  4i;   // long     → FLOAT (Perspective has no int64)
.psp.t.typeMap[8j]:  4i;   // real     → FLOAT
.psp.t.typeMap[9j]:  4i;   // float    → FLOAT
.psp.t.typeMap[10j]: 0i;   // char     → STRING
.psp.t.typeMap[11j]: 0i;   // symbol   → STRING
.psp.t.typeMap[12j]: 2i;   // timestamp→ DATETIME
.psp.t.typeMap[13j]: 1i;   // month    → DATE
.psp.t.typeMap[14j]: 1i;   // date     → DATE
.psp.t.typeMap[15j]: 2i;   // datetime → DATETIME
.psp.t.typeMap[16j]: 2i;   // timespan → DATETIME
.psp.t.typeMap[17j]: 3i;   // minute   → INTEGER
.psp.t.typeMap[18j]: 3i;   // second   → INTEGER
.psp.t.typeMap[19j]: 3i;   // time     → INTEGER
// GUID (2) → STRING
.psp.t.typeMap[2j]:  0i;

// Enum types (20-76) → STRING
.psp.t.typeMap[,] ./: ((!57) + 20j)!57#0i;

// ---------------------------------------------------------------------------
// .psp.t.kdbTypeToColType[t]
// Convert a KDB+ type code (as returned by `type col`) to Perspective int.
// Accepts both atom (negative) and vector (positive) type codes.
// ---------------------------------------------------------------------------
.psp.t.kdbTypeToColType: {[t]
    tc: abs "j"$t;
    $[tc in key .psp.t.typeMap;
        .psp.t.typeMap tc;
        0i]   // fallback → STRING
 };

// ---------------------------------------------------------------------------
// .psp.t.schemaFromMeta[m]
// Convert output of `meta table` to a sym→ColumnType dict.
// m: keyed table with columns: c (sym), t (char), f (sym), a (sym)
// Returns: `col_name1`col_name2!0i 4i ...  (sym!int vector)
// ---------------------------------------------------------------------------
.psp.t.schemaFromMeta: {[m]
    cols_list: exec c from m;
    type_chars: exec t from m;
    // type char to KDB+ type code: use .Q.t inverse mapping
    // .Q.t is a 256-char string where .Q.t[typeCode] = typeChar
    // We need typeChar → typeCode: build reverse map once
    tc: .psp.t.charToCode each type_chars;
    ct: .psp.t.kdbTypeToColType each tc;
    cols_list ! ct
 };

// Map type character (lowercase) to vector type code (positive short)
// Uses the built-in .Q.t mapping: .Q.t is indexed by type code
.psp.t.charToCode: {[c]
    // .Q.t is a 256-element string, index = type code (0..76 positive for vectors)
    // find c in .Q.t vector range (1..20 covers common types)
    idx: first where .Q.t = c;
    $[null idx; 11j; "j"$idx]   // default → symbol (11) if not found
 };
