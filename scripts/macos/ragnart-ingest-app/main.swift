// RAGNART Ingest — launcher for the RAGNART endpoint runner on macOS.
//
// macOS charges network requests (Local Network privacy) and background activity to the app that
// launched a process. Running the venv's Python directly makes those entries read "python3.12",
// shared by every Python program on the Mac. This launcher gives the runner its own identity,
// "RAGNART Ingest" (bundle id com.ragnart.ingest): it starts
//     ~/.python-3.12/bin/python -m ragnart.endpoint <its own arguments>
// as a child, waits for it, and exits with the child's exit status.
import Foundation

let home = FileManager.default.homeDirectoryForCurrentUser.path
let python = home + "/.python-3.12/bin/python"
let child = Process()
child.executableURL = URL(fileURLWithPath: python)
child.arguments = ["-m", "ragnart.endpoint"] + Array(CommandLine.arguments.dropFirst())
do {
    try child.run()
} catch {
    FileHandle.standardError.write("RAGNART Ingest: cannot start \(python): \(error)\n".data(using: .utf8)!)
    exit(127)
}
child.waitUntilExit()
exit(child.terminationStatus)
