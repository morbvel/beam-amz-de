an# Amz-DE
## Introduction

This Code is the migration from SQL to Scala code. The performance was improved with this migration

## Installation

Before install, you have to create a file named ".credentials" inside the ivy2 directory. In this file, you have to create this information: 

realm=Sonatype Nexus Repository Manager
host=104.154.138.29
user=beam-nexus
password=Nexus@321

This credentials are used for download our libraries.

Usual Commands:
 
- sbt clean
- sbt compile
- sbt test
- sbt publish

if you want to run the test on a Windows machine, you could need:

1. Download the following file: https://github.com/steveloughran/winutils/blob/master/hadoop-2.6.3/bin/winutils.exe
2. Move the file to C:\winutils\bin

## Log version

Version: 1.0.0-00-SNAPSHOT

    Features:
        Migration to Scala.
    Package dependencies:    
        Spark 2.2.0
        Scala Test 2.2.2 
        Junit 4.12
        Scala Commons 1.0.0-03-SNAPSHOT
        The Bar Global 1.0.0-00
