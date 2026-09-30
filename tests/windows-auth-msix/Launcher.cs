using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Web.Script.Serialization;

// Compiled by the smoke controller with the Windows .NET Framework compiler.
public static class Launcher
{
    private const int NoPackage = 15700;
    private const int InsufficientBuffer = 122;
    private const int TimeoutMilliseconds = 90000;
    private static readonly Encoding Utf8 = new UTF8Encoding(false, true);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true)]
    private static extern int GetCurrentPackageFullName(ref uint length, StringBuilder name);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true)]
    private static extern int GetPackageFullName(IntPtr process, ref uint length, StringBuilder name);

    public sealed class Request
    {
        public string action { get; set; }
        public string legacyDir { get; set; }
        public string authJson { get; set; }
        public string secretsJson { get; set; }
        public string btPath { get; set; }
        public string[] arguments { get; set; }
        public string workingDirectory { get; set; }
        public Dictionary<string, string> environment { get; set; }
        public string resultPath { get; set; }
    }

    public sealed class Result
    {
        public bool success { get; set; }
        public string error { get; set; }
        public string packageFullName { get; set; }
        public string childPackageFullName { get; set; }
        public int? exitCode { get; set; }
        public string stdout { get; set; }
        public string stderr { get; set; }
        public string authJson { get; set; }
        public string secretsJson { get; set; }
    }

    public static int Main(string[] args)
    {
        Request request = null;
        Result result = new Result();
        JavaScriptSerializer serializer = new JavaScriptSerializer();
        try
        {
            if (args.Length != 1)
                throw new ArgumentException("Expected one JSON request file path.");
            request = serializer.Deserialize<Request>(File.ReadAllText(args[0], Utf8));
            if (request == null)
                throw new ArgumentException("The request must be a JSON object.");
            Require(request.resultPath, "resultPath");
            result.packageFullName = CurrentPackageName();
            switch (request.action)
            {
                case "seed":
                    Require(request.legacyDir, "legacyDir");
                    if (request.authJson == null || request.secretsJson == null)
                        throw new ArgumentException("seed requires authJson and secretsJson.");
                    Directory.CreateDirectory(request.legacyDir);
                    File.WriteAllText(Path.Combine(request.legacyDir, "auth.json"), request.authJson, Utf8);
                    File.WriteAllText(Path.Combine(request.legacyDir, "secrets.json"), request.secretsJson, Utf8);
                    ReadLegacy(request, result);
                    break;
                case "probe":
                    Require(request.legacyDir, "legacyDir");
                    ReadLegacy(request, result);
                    break;
                case "run":
                    Run(request, result);
                    break;
                default:
                    throw new ArgumentException("action must be seed, probe, or run.");
            }
            result.success = true;
        }
        catch (Exception error)
        {
            result.success = false;
            result.error = error.GetType().Name + ": " + error.Message;
        }

        // Never print requests, credentials, environment variables, or child output.
        // A malformed request without a result path can only be signaled by exit code.
        if (request == null || String.IsNullOrWhiteSpace(request.resultPath))
            return 1;
        try
        {
            Publish(request.resultPath, serializer.Serialize(result));
        }
        catch
        {
            return 1;
        }
        return result.success ? 0 : 1;
    }

    private static void Require(string value, string field)
    {
        if (String.IsNullOrWhiteSpace(value))
            throw new ArgumentException(field + " is required.");
    }

    private static void ReadLegacy(Request request, Result result)
    {
        result.authJson = ReadOptional(Path.Combine(request.legacyDir, "auth.json"));
        result.secretsJson = ReadOptional(Path.Combine(request.legacyDir, "secrets.json"));
    }

    private static string ReadOptional(string path)
    {
        try
        {
            return File.ReadAllText(path, Utf8);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }

    private static string CurrentPackageName()
    {
        uint length = 0;
        int status = GetCurrentPackageFullName(ref length, null);
        if (status == NoPackage)
            return null;
        CheckStatus(status, InsufficientBuffer, "GetCurrentPackageFullName(size)");
        StringBuilder name = new StringBuilder(checked((int)length));
        status = GetCurrentPackageFullName(ref length, name);
        if (status == NoPackage)
            return null;
        CheckStatus(status, 0, "GetCurrentPackageFullName");
        return name.ToString();
    }

    private static string ChildPackageName(IntPtr process)
    {
        uint length = 0;
        int status = GetPackageFullName(process, ref length, null);
        if (status == NoPackage)
            return null;
        CheckStatus(status, InsufficientBuffer, "GetPackageFullName(size)");
        StringBuilder name = new StringBuilder(checked((int)length));
        status = GetPackageFullName(process, ref length, name);
        if (status == NoPackage)
            return null;
        CheckStatus(status, 0, "GetPackageFullName");
        return name.ToString();
    }

    private static void CheckStatus(int actual, int expected, string operation)
    {
        if (actual != expected)
            throw new Win32Exception(actual, operation + " failed with Windows error " + actual + ".");
    }

    private static void Run(Request request, Result result)
    {
        Require(request.btPath, "btPath");
        Require(request.workingDirectory, "workingDirectory");
        ProcessStartInfo start = new ProcessStartInfo();
        start.FileName = request.btPath;
        start.Arguments = QuoteArguments(request.arguments);
        start.WorkingDirectory = request.workingDirectory;
        start.UseShellExecute = false;
        start.CreateNoWindow = true;
        start.RedirectStandardOutput = true;
        start.RedirectStandardError = true;
        start.StandardOutputEncoding = Utf8;
        start.StandardErrorEncoding = Utf8;
        if (request.environment != null)
        {
            foreach (KeyValuePair<string, string> entry in request.environment)
            {
                if (entry.Value == null)
                    start.EnvironmentVariables.Remove(entry.Key);
                else
                    start.EnvironmentVariables[entry.Key] = entry.Value;
            }
        }

        using (Process child = new Process())
        {
            child.StartInfo = start;
            if (!child.Start())
                throw new InvalidOperationException("Starting bt did not create a process.");
            try
            {
                // With UseShellExecute=false, Process.Start retains CreateProcess's
                // handle. Do not reopen by PID: a short-lived child may already exit.
                IntPtr handle = child.Handle;
                result.childPackageFullName = ChildPackageName(handle);
                Stopwatch elapsed = Stopwatch.StartNew();
                Task<string> stdout = child.StandardOutput.ReadToEndAsync();
                Task<string> stderr = child.StandardError.ReadToEndAsync();
                if (!child.WaitForExit(TimeoutMilliseconds))
                    throw new TimeoutException("bt exceeded the 90 second execution deadline.");
                result.exitCode = child.ExitCode;
                int remaining = Math.Max(0, TimeoutMilliseconds - (int)elapsed.ElapsedMilliseconds);
                if (!Task.WaitAll(new Task[] { stdout, stderr }, remaining))
                    throw new TimeoutException("bt output streams exceeded the 90 second execution deadline.");
                result.stdout = stdout.Result;
                result.stderr = stderr.Result;
            }
            finally
            {
                if (!child.HasExited)
                {
                    child.Kill();
                    if (!child.WaitForExit(5000))
                        throw new TimeoutException("bt did not terminate within 5 seconds after being killed.");
                }
            }
        }
    }

    private static string QuoteArguments(string[] arguments)
    {
        if (arguments == null)
            return String.Empty;
        StringBuilder commandLine = new StringBuilder();
        foreach (string argument in arguments)
        {
            if (argument == null)
                throw new ArgumentException("arguments cannot contain null values.");
            if (commandLine.Length != 0)
                commandLine.Append(' ');
            commandLine.Append('"');
            int slashes = 0;
            foreach (char character in argument)
            {
                if (character == '\\')
                {
                    slashes++;
                    continue;
                }
                if (character == '"')
                {
                    commandLine.Append('\\', slashes * 2 + 1);
                    commandLine.Append('"');
                }
                else
                {
                    commandLine.Append('\\', slashes);
                    commandLine.Append(character);
                }
                slashes = 0;
            }
            commandLine.Append('\\', slashes * 2);
            commandLine.Append('"');
        }
        return commandLine.ToString();
    }

    private static void Publish(string path, string json)
    {
        string temporary = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            File.WriteAllText(temporary, json, Utf8);
            if (File.Exists(path))
                File.Replace(temporary, path, null);
            else
                File.Move(temporary, path);
        }
        finally
        {
            if (File.Exists(temporary))
                File.Delete(temporary);
        }
    }
}
