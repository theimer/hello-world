import Foundation
import SQLite3

/// Errors thrown by ``Database`` and the helpers that operate on it.
public enum DatabaseError: Error, CustomStringConvertible {
    case openFailed(path: String, message: String)
    case prepareFailed(sql: String, message: String)
    case stepFailed(sql: String, message: String)

    public var description: String {
        switch self {
        case let .openFailed(path, msg):
            return "open(\(path)): \(msg)"
        case let .prepareFailed(sql, msg):
            return "prepare(\(sql)): \(msg)"
        case let .stepFailed(sql, msg):
            return "step(\(sql)): \(msg)"
        }
    }
}

/// SQLite literal string used as ``sqlite3_bind_text``'s destructor
/// argument so SQLite copies the buffer rather than retaining our
/// transient Swift string's storage.
private let SQLITE_TRANSIENT = unsafeBitCast(
    OpaquePointer(bitPattern: -1), to: sqlite3_destructor_type.self)

/// Thin SQLite wrapper.
///
/// Takes a path, opens a connection, exposes minimal `exec` /
/// `query` / `prepared` helpers.  Designed for the small, fixed query
/// surface the host and verifier need — not a general-purpose ORM.
///
/// Thread-safety: the underlying connection is single-threaded.
/// host.py-equivalent code paths open/use/close the DB inside a single
/// invocation, so concurrent use is not a concern.
public final class Database {

    private var handle: OpaquePointer?

    public init(path: String) throws {
        var dbPtr: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
        let rc = sqlite3_open_v2(path, &dbPtr, flags, nil)
        guard rc == SQLITE_OK, let h = dbPtr else {
            let msg = dbPtr.map {
                String(cString: sqlite3_errmsg($0))
            } ?? "sqlite3_open_v2 returned \(rc)"
            sqlite3_close(dbPtr)
            throw DatabaseError.openFailed(path: path, message: msg)
        }
        self.handle = h
    }

    deinit {
        if let h = handle {
            sqlite3_close(h)
        }
    }

    /// Run an SQL statement with no return value (CREATE / INSERT /
    /// UPDATE / DELETE).  Multiple semicolon-separated statements are
    /// supported.
    public func execute(_ sql: String) throws {
        var errPtr: UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(handle, sql, nil, nil, &errPtr)
        if rc != SQLITE_OK {
            let msg = errPtr.map { String(cString: $0) } ?? "rc=\(rc)"
            sqlite3_free(errPtr)
            throw DatabaseError.stepFailed(sql: sql, message: msg)
        }
    }

    /// Bind one of the supported scalar types to a prepared statement
    /// at 1-based column `idx`.  Nil binds NULL.
    fileprivate func bind(_ stmt: OpaquePointer, _ idx: Int32, _ value: Any?) {
        switch value {
        case nil:
            sqlite3_bind_null(stmt, idx)
        case let s as String:
            sqlite3_bind_text(stmt, idx, s, -1, SQLITE_TRANSIENT)
        case let i as Int:
            sqlite3_bind_int64(stmt, idx, Int64(i))
        case let i as Int64:
            sqlite3_bind_int64(stmt, idx, i)
        case let d as Double:
            sqlite3_bind_double(stmt, idx, d)
        default:
            // Unknown type; bind its description as a string.  Dev-only
            // safety net — the helpers in this file always pass a known
            // primitive.
            sqlite3_bind_text(
                stmt, idx, String(describing: value!), -1, SQLITE_TRANSIENT)
        }
    }

    /// Run a parameterised statement that returns no rows (INSERT /
    /// UPDATE / DELETE).  Returns the number of rows changed.
    @discardableResult
    public func run(_ sql: String, _ params: [Any?] = []) throws -> Int {
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(handle, sql, -1, &stmt, nil) == SQLITE_OK else {
            let msg = String(cString: sqlite3_errmsg(handle))
            throw DatabaseError.prepareFailed(sql: sql, message: msg)
        }
        defer { sqlite3_finalize(stmt) }
        for (i, p) in params.enumerated() {
            bind(stmt!, Int32(i + 1), p)
        }
        let rc = sqlite3_step(stmt)
        guard rc == SQLITE_DONE || rc == SQLITE_ROW else {
            let msg = String(cString: sqlite3_errmsg(handle))
            throw DatabaseError.stepFailed(sql: sql, message: msg)
        }
        return Int(sqlite3_changes(handle))
    }

    /// Run a parameterised statement and call `consume` on each
    /// resulting row.  `consume` receives a `Row` cursor with typed
    /// accessors by 0-based column index.
    public func query(
        _ sql: String,
        _ params: [Any?] = [],
        consume: (Row) throws -> Void
    ) throws {
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(handle, sql, -1, &stmt, nil) == SQLITE_OK else {
            let msg = String(cString: sqlite3_errmsg(handle))
            throw DatabaseError.prepareFailed(sql: sql, message: msg)
        }
        defer { sqlite3_finalize(stmt) }
        for (i, p) in params.enumerated() {
            bind(stmt!, Int32(i + 1), p)
        }
        while true {
            let rc = sqlite3_step(stmt)
            if rc == SQLITE_DONE { break }
            guard rc == SQLITE_ROW else {
                let msg = String(cString: sqlite3_errmsg(handle))
                throw DatabaseError.stepFailed(sql: sql, message: msg)
            }
            try consume(Row(stmt: stmt!))
        }
    }

    /// Convenience: collect all rows by mapping each Row to a value.
    public func queryAll<T>(
        _ sql: String,
        _ params: [Any?] = [],
        map: (Row) throws -> T
    ) throws -> [T] {
        var out: [T] = []
        try query(sql, params) { row in out.append(try map(row)) }
        return out
    }

    /// Convenience: return at most one row (the first), nil if no rows.
    public func queryOne<T>(
        _ sql: String,
        _ params: [Any?] = [],
        map: (Row) throws -> T
    ) throws -> T? {
        var result: T?
        try query(sql, params) { row in
            if result == nil { result = try map(row) }
        }
        return result
    }

    /// One row from a query result.  Light wrapper around `sqlite3_stmt`
    /// that only exposes typed column accessors — no leaking of the
    /// underlying pointer.
    public struct Row {
        let stmt: OpaquePointer

        /// String column value, defaulting to "" if NULL.
        public func string(_ idx: Int) -> String {
            guard let cstr = sqlite3_column_text(stmt, Int32(idx)) else {
                return ""
            }
            return String(cString: cstr)
        }

        /// Int column value, defaulting to 0 if NULL.
        public func int(_ idx: Int) -> Int {
            Int(sqlite3_column_int64(stmt, Int32(idx)))
        }

        /// Optional string column — nil if NULL.
        public func optionalString(_ idx: Int) -> String? {
            if sqlite3_column_type(stmt, Int32(idx)) == SQLITE_NULL {
                return nil
            }
            return string(idx)
        }
    }
}
