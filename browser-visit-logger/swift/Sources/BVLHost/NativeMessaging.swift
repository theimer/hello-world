import Foundation

/// Errors raised by the native-messaging stdio framing layer.
public enum NativeMessagingError: Error, CustomStringConvertible {
    case eofBeforeLength
    case eofMidMessage
    case decodeFailed(String)
    case encodeFailed(String)

    public var description: String {
        switch self {
        case .eofBeforeLength:
            return "stdin closed before length header was complete"
        case .eofMidMessage:
            return "stdin closed mid-message"
        case let .decodeFailed(msg):
            return "JSON decode failed: \(msg)"
        case let .encodeFailed(msg):
            return "JSON encode failed: \(msg)"
        }
    }
}

/// Chrome native messaging stdio protocol: 4-byte little-endian length
/// header followed by the UTF-8 JSON payload.  One message in, one
/// message out, then exit (Chrome MV3's one-shot semantics).
public enum NativeMessaging {

    /// Read one framed message from stdin.  Returns the decoded JSON
    /// object as `[String: Any]`; throws on EOF or malformed framing.
    public static func read() throws -> [String: Any] {
        let stdin = FileHandle.standardInput
        let header = stdin.readData(ofLength: 4)
        guard header.count == 4 else {
            throw NativeMessagingError.eofBeforeLength
        }
        let length = Int(header.withUnsafeBytes { raw -> UInt32 in
            raw.load(as: UInt32.self)
        }.littleEndian)
        let body = stdin.readData(ofLength: length)
        guard body.count == length else {
            throw NativeMessagingError.eofMidMessage
        }
        let obj: Any
        do {
            obj = try JSONSerialization.jsonObject(with: body, options: [])
        } catch {
            throw NativeMessagingError.decodeFailed("\(error)")
        }
        guard let dict = obj as? [String: Any] else {
            throw NativeMessagingError.decodeFailed("not an object")
        }
        return dict
    }

    /// Write one framed message to stdout.  Errors writing the data
    /// are silently ignored — Chrome will close the pipe on its end if
    /// the parent has gone away, and there's nothing useful we can do.
    public static func write(_ payload: [String: Any]) throws {
        let body: Data
        do {
            body = try JSONSerialization.data(
                withJSONObject: payload,
                options: [.fragmentsAllowed])
        } catch {
            throw NativeMessagingError.encodeFailed("\(error)")
        }
        var lengthLE = UInt32(body.count).littleEndian
        let header = withUnsafeBytes(of: &lengthLE) { Data($0) }
        let stdout = FileHandle.standardOutput
        try stdout.write(contentsOf: header)
        try stdout.write(contentsOf: body)
    }
}
