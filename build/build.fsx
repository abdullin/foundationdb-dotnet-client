// include Fake lib
#r "tools/FAKE/tools/FakeLib.dll"

open Fake

RestorePackages()

// Properties
let version = "0.200.6-pre" //TODO: find a way to extract this from somewhere convenient
let buildDir = "./build/output/"
let nugetPath = ".nuget/nuget.exe"
let nugetOutDir = buildDir + "_packages/"

let BuildProperties =
    [
        "Configuration", "Release"
        "TargetPlatform", "AnyCPU"
        "AllowUnsafeBlocks", "true"
    ]

Target "Clean" (fun _ -> 
    CleanDir buildDir
)
// Default target
Target "Build" (fun _ -> traceHeader "STARTING BUILD")

Target "BuildApp" (fun _ ->
    let binDirs =
        !! "**/bin/**"
        |> Seq.map DirectoryName
        |> Seq.distinct
        |> Seq.filter (fun f -> (f.EndsWith("Debug") || f.EndsWith("Release")) && not (f.Contains "CodeGeneration"))

    CleanDirs binDirs

    //Compile each csproj and output it seperately in build/output/PROJECTNAME
    !! "**/*.csproj"
    |> Seq.map(fun f -> (f, buildDir + directoryInfo(f).Name.Replace(".csproj", "")))
    |> Seq.iter(fun (f,d) -> MSBuild d "Build" BuildProperties (seq { yield f }) |> ignore)
)

Target "Test" (fun _ ->
    let testDir = buildDir + "tests/"
    CreateDir testDir
    !!(buildDir + "**/*Test*.dll")
    |> NUnit(
        fun p -> { p with DisableShadowCopy = true
                          OutputFile = "TestResults.xml"
                          StopOnError = false
                          ErrorLevel = DontFailBuild
                          WorkingDir = testDir
                          ExcludeCategory = "LongRunning,LocalCluster" }))

Target "Release" (fun _ ->
    traceHeader "BUILDING RELEASE"
)

let replaceVersionInNuspec nuspecFileName version =
    let re = @"(?<start>\<version\>|""FoundationDB.Client""\s?version="")[^""<>]+(?<end>\<\/version\>|"")"
    let nuspecContents = ReadFileAsString nuspecFileName
    let replacedContents = regex_replace re (sprintf "${start}%s${end}" version) nuspecContents
    WriteStringToFile false nuspecFileName replacedContents

Target "BuildNuget" (fun _ ->
    trace "Building Nuget Packages"
    let projects = [ "FoundationDb.Client"; "FoundationDb.Layers.Common" ]   
    CreateDir nugetOutDir
    projects
    |> List.iter (
        fun name ->
            let nuspec = sprintf @"build\%s.nuspec" name
            replaceVersionInNuspec nuspec version
            let binariesDir = sprintf "%s/%s/" buildDir name
            NuGetPack (
                fun p ->
                    { p with WorkingDir = binariesDir
                             OutputPath = nugetOutDir
                             ToolPath = nugetPath
                             Version = version}) nuspec

            let targetLoc = (buildDir + (sprintf "%s/%s.%s.nupkg" name name version))
            trace targetLoc

        (*MoveFile nugetOutDir (buildDir + (sprintf "%s/%s/.%s.nupkg" name name version))*)
    )
)

Target "Default" (fun _ -> trace "Starting build")

// Dependencies
"Clean" ==> "BuildApp" ==> "Test" ==> "Build"
"Clean" ==> "BuildApp" ==> "BuildNuget" ==> "Release"

// start build
RunTargetOrDefault "Build"
